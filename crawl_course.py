import asyncio
import io
import logging
from typing import Any, Dict, List, Optional

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from config import config
from db import (
    save_course_detail_to_db,
    save_course_info_to_db,
    save_merged_courses_to_db,
)
from utils.dataframe_utils import process_course_info_df

from utils.logger import setup_logger, get_logger

# Ensure logger is set up when running as script or importing
# Since this script is often run as a subprocess, we should set up logging here too if main check passes,
# but for global scope safety, we can just call setup_logger if it's the main entry or ensure it's idempotent.
# setup_logger() uses basicConfig so it is idempotent.
setup_logger()
logger = get_logger(__name__)

CONCURRENCY_LIMIT = 5


async def main() -> None:
    """獲取課程資訊和詳細資訊並整合為一張表"""
    logger.info("[crawl_course] Start executing course crawler")

    try:
        # --- 1. 爬取課程基本資訊 ---
        logger.info("[crawl_course] fetching course basic info...")
        course_info_df = await fetch_course_info(config.academic_year, config.academic_semester)

        if course_info_df.empty:
            logger.error(
                "[crawl_course] Failed to fetch course info, terminating program."
            )
            return

        course_info_df = process_course_info_df(course_info_df)
        save_course_info_to_db(course_info_df)
        logger.info(f"[crawl_course] Done! Saved {len(course_info_df)} courses")

        # --- 2. 爬取課程詳細資訊 ---
        logger.info("[crawl_course] fetching course details...")
        course_codes = course_info_df["course_code"].tolist()

        if config.db_env == "dev":
            course_codes = course_codes[:config.dev_data_limit]
            logger.warning(f"[DEV MODE] Fetching {config.dev_data_limit} course details")

        # 呼叫並發爬蟲函式
        course_detail_df = await fetch_course_details_concurrently(
            config.academic_year, config.academic_semester, course_codes
        )

        save_course_detail_to_db(course_detail_df)
        logger.info(f"[crawl_course] Done! Saved {len(course_detail_df)} courses")

        # --- 3. 資料整併 (Merge) ---
        logger.info("[crawl_course] merging dataframes...")
        merged_df = pd.merge(
            course_info_df, course_detail_df, on="course_code", how="left"
        )

        logger.info(f"[crawl_course] Done! Merged {len(merged_df)} courses")
        save_merged_courses_to_db(merged_df)

        logger.info("[crawl_course] Done! Saved merged courses")

    except Exception as e:
        logger.error(f"Course crawling failed: {e}")
        import traceback

        traceback.print_exc()

    logger.info("[crawl_course] Course crawling completed!")


async def fetch_course_info(academic_year: str, academic_semester: str) -> pd.DataFrame:
    """獲取課程基本資訊 (使用 aiohttp)"""
    url = f"https://course.thu.edu.tw/opendatadownload/list/{academic_year}/{academic_semester}/"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                text = await response.text()
                df = pd.read_csv(
                    io.StringIO(text),
                    dtype={"選課代碼": str, "開課系所代碼": str},
                    on_bad_lines="skip",
                )
                return df
    except Exception as e:
        logger.error(f"Error fetching course info: {e}")
        return pd.DataFrame()


async def fetch_single_course_detail(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    academic_year: str,
    academic_semester: str,
    course_code: str,
) -> Optional[Dict[str, Any]]:
    """
    爬取「單一」課程詳細資訊的邏輯
    """
    url = f"https://course.thu.edu.tw/view/{academic_year}/{academic_semester}/{course_code}/"

    async with semaphore:
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    # 失敗時可以 print，但建議使用 logging 避免干擾進度條
                    # print(f"Failed to fetch {course_code}, status: {response.status}")
                    return None
                html = await response.text()

            # --- BeautifulSoup 解析邏輯 ---
            soup = BeautifulSoup(html, "html.parser")

            closed_notice = soup.find(class_="warning closable")
            if closed_notice:
                return {"course_code": course_code, "is_closed": True}

            table = soup.find("table")
            if not isinstance(table, Tag):
                # print(f"No table found for course {course_code}")
                return None

            rows = table.find_all("tr")
            grading_data = []

            for row in rows:
                if isinstance(row, Tag):
                    cols = [
                        col.get_text(strip=True) for col in row.find_all(["td", "th"])
                    ]
                    if cols:
                        grading_data.append(cols)

            grading_items = []
            if grading_data and len(grading_data) > 1:
                for row in grading_data[1:]:
                    if len(row) >= 3:
                        grading_item = {
                            "method": row[0],
                            "percentage": row[1],
                            "description": row[2] if len(row) > 2 else "",
                        }
                        grading_items.append(grading_item)

            selection_records = []
            scripts = soup.find_all("script")
            for script in scripts:
                if isinstance(script, Tag):
                    script_text = script.get_text()
                    if "google.visualization.arrayToDataTable" in script_text:
                        import re

                        pattern = r"google\.visualization\.arrayToDataTable\(\s*\[\s*([\s\S]*?)\s*\]\s*\)"
                        match = re.search(pattern, script_text, re.DOTALL)
                        if match:
                            data_content = match.group(1)
                            row_pattern = (
                                r"\[\s*'([^']+)',\s*(\d+),\s*(\d+),\s*(\d+)\s*\]"
                            )
                            matches = re.findall(row_pattern, data_content)
                            for match_data in matches:
                                date, enrolled, remaining, registered = match_data
                                selection_records.append(
                                    {
                                        "date": date,
                                        "enrolled": int(enrolled),
                                        "remaining": int(remaining),
                                        "registered": int(registered),
                                    }
                                )
                            break

            teacher_list = []
            teacher_section = soup.select_one(
                "#mainContent > div:nth-child(4) > div:nth-child(1)"
            )
            if teacher_section:
                teacher_links = teacher_section.find_all("a")
                teacher_list = [
                    a.get_text(strip=True) if a.get_text(strip=True) != "" else None
                    for a in teacher_links
                ]

            teaching_goal = None
            course_description = None

            content_div = soup.select_one(
                "#mainContent > div:nth-child(4) > div.thirteen.columns"
            )
            if content_div:
                for h2 in content_div.find_all("h2", class_="title"):
                    title_text = h2.get_text(strip=True)
                    next_p = h2.find_next_sibling("p")
                    if next_p:
                        content = next_p.get_text(strip=True)
                        if "教育目標" in title_text:
                            teaching_goal = content
                        elif "課程概述" in title_text:
                            course_description = content

            basic_info = {}
            basic_info_element = soup.select_one(
                "#mainContent > div:nth-child(5) > div:nth-child(2) > div:nth-child(1) > p"
            )
            if basic_info_element:
                parts = []
                for element in basic_info_element.contents:
                    if isinstance(element, NavigableString):
                        text = element.strip()
                        if text:
                            parts.append(text)
                    elif isinstance(element, Tag) and element.name == "br":
                        parts.append("\n")

                basic_info_text = "".join(parts)
                lines = [
                    line.strip() for line in basic_info_text.split("\n") if line.strip()
                ]

                for line in lines:
                    if "：" in line:
                        key, value = line.split("：", 1)
                        key = key.strip()
                        value = value.strip()
                        if key in ["選修課", "必修課"]:
                            basic_info["course_type"] = key
                            if "，學分數" in line:
                                credits_part = (
                                    line.split("，學分數：")[1]
                                    if "，學分數：" in line
                                    else value
                                )
                                basic_info["credits"] = credits_part.strip()
                        elif key == "學分數":
                            basic_info["credits"] = value
                        elif key == "上課時間":
                            basic_info["class_time"] = value
                        elif key == "修課班級":
                            basic_info["target_class"] = value
                        elif key == "修課年級":
                            basic_info["target_grade"] = value
                        elif key == "選課備註":
                            basic_info["enrollment_notes"] = value

                if not basic_info:
                    basic_info["raw_text"] = basic_info_text

            # 重要：將原本的 print 移除，以免干擾進度條顯示
            # print(f"Success fetching course detail for {course_code}")
            return {
                "course_code": course_code,
                "is_closed": bool(closed_notice),
                "teachers": teacher_list,
                "grading_items": grading_items,
                "selection_records": selection_records,
                "teaching_goal": teaching_goal,
                "course_description": course_description,
                "basic_info": basic_info,
            }

        except Exception as e:
            # 使用 print 會破壞進度條，實務上建議收集錯誤最後顯示，或寫入 log 檔
            # print(f"Error processing {course_code}: {e}")
            return None


async def fetch_course_details_concurrently(
    academic_year: str, academic_semester: str, course_codes: List[str]
) -> pd.DataFrame:
    """
    管理所有並發任務的函式，並加入 Rich Progress Bar
    """

    # 定義進度條樣式
    progress = Progress(
        SpinnerColumn(),  # 轉圈圈動畫
        TextColumn("[progress.description]{task.description}"),  # 任務描述
        BarColumn(),  # 進度條本體
        TaskProgressColumn(),  # 百分比 (e.g., 50%)
        MofNCompleteColumn(),  # 完成數/總數 (e.g., 1500/3000)
        TimeElapsedColumn(),  # 已過時間
        TimeRemainingColumn(),  # 剩餘時間估算
    )

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async with aiohttp.ClientSession() as session:
        # 使用 progress context manager
        with progress:
            task_id = progress.add_task(
                f"[cyan]fetching course details (concurrency: {CONCURRENCY_LIMIT})...",
                total=len(course_codes),
            )

            # 建立一個 wrapper 來處理單個任務完成後的進度更新
            async def worker(code: str):
                result = await fetch_single_course_detail(
                    session, semaphore, academic_year, academic_semester, code
                )
                # 每完成一個任務，進度條 +1
                progress.advance(task_id)
                return result

            # 建立所有 worker 任務
            tasks = [worker(code) for code in course_codes]

            # 等待所有任務完成
            results = await asyncio.gather(*tasks)

    # 過濾掉失敗的 (None) 結果
    valid_results = [r for r in results if r is not None]

    return pd.DataFrame(valid_results)


if __name__ == "__main__":
    asyncio.run(main())
