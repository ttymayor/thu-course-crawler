import asyncio
import io
import logging
import re
from typing import Any, Dict, List, Optional

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from bs4.element import Tag
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
from db import save_merged_courses_to_db
from utils.dataframe_utils import process_course_info_df

from utils.logger import setup_logger, get_logger

# Ensure logger is set up when running as script or importing
# Since this script is often run as a subprocess, we should set up logging here too if main check passes,
# but for global scope safety, we can just call setup_logger if it's the main entry or ensure it's idempotent.
# setup_logger() uses basicConfig so it is idempotent.
setup_logger()
logger = get_logger(__name__)

BASE_URL = "https://course.thu.edu.tw"


def clean_text(element: Optional[Tag]) -> str:
    """Return compact visible text from a BeautifulSoup element."""
    if not element:
        return ""
    return re.sub(r"\s+", " ", element.get_text(" ", strip=True)).strip()


def text_after_label(text: str, label: str) -> str:
    return re.sub(r"\s+", " ", text.replace(label, "", 1)).strip()


def extract_card_value(soup: BeautifulSoup, label: str) -> str:
    for card in soup.select(".card"):
        heading = card.select_one("h6.card-title")
        if heading and label in heading.get_text(strip=True):
            return text_after_label(clean_text(card.select_one(".card-body")), label)
    return ""


def extract_teachers(soup: BeautifulSoup) -> list[str]:
    for card in soup.select(".card"):
        heading = card.select_one("h6.card-title")
        if not heading or "授課教師" not in heading.get_text(strip=True):
            continue
        teachers = [
            clean_text(anchor)
            for anchor in card.find_all("a")
            if clean_text(anchor)
        ]
        if teachers:
            return teachers
        body_text = extract_card_value(soup, "授課教師")
        return [name.strip() for name in re.split(r"[/、,，]", body_text) if name.strip()]
    return []


def extract_hero_basic_info(soup: BeautifulSoup) -> dict[str, str]:
    basic_info: dict[str, str] = {}

    hero = soup.select_one("#course-hero")
    if hero:
        for badge in hero.select(".badge"):
            badge_text = clean_text(badge)
            if "必修" in badge_text or "選修" in badge_text:
                basic_info["course_type"] = badge_text
            elif "學分" in badge_text:
                basic_info["credits"] = badge_text.replace("學分", "").strip()

    class_time = extract_card_value(soup, "上課時間")
    target_class = extract_card_value(soup, "修課班級")
    enrollment_notes = extract_card_value(soup, "課程資訊")

    if class_time:
        basic_info["class_time"] = class_time
    if target_class:
        basic_info["target_class"] = target_class
    if enrollment_notes:
        basic_info["enrollment_notes"] = enrollment_notes

    return basic_info


def extract_accordion_section(soup: BeautifulSoup, label: str) -> str:
    accordion = soup.select_one("#courseDetailsAccordion")
    if not accordion:
        return ""

    for item in accordion.select(".accordion-item"):
        button = item.select_one(".accordion-button")
        if not button or label not in clean_text(button):
            continue
        body = item.select_one(".accordion-body")
        return clean_text(body)
    return ""


def extract_grading_items(soup: BeautifulSoup) -> list[dict[str, str]]:
    grading_items: list[dict[str, str]] = []
    accordion = soup.select_one("#courseDetailsAccordion")
    if not accordion:
        return grading_items

    grading_item = None
    for item in accordion.select(".accordion-item"):
        button = item.select_one(".accordion-button")
        if button and "評分方式" in clean_text(button):
            grading_item = item
            break

    if not grading_item:
        return grading_items

    for row in grading_item.select("tr"):
        cols = [clean_text(col) for col in row.find_all(["td", "th"])]
        cols = [col for col in cols if col]
        if len(cols) >= 2 and not any("評分" in col for col in cols[:1]):
            grading_items.append(
                {
                    "method": cols[0],
                    "percentage": cols[1],
                    "description": cols[2] if len(cols) > 2 else "",
                }
            )

    return grading_items


def extract_selection_records(soup: BeautifulSoup) -> list[dict[str, Any]]:
    selection_records: list[dict[str, Any]] = []
    scripts = soup.find_all("script")
    for script in scripts:
        if not isinstance(script, Tag):
            continue
        script_text = script.get_text()
        if "google.visualization.arrayToDataTable" not in script_text:
            continue
        pattern = r"google\.visualization\.arrayToDataTable\(\s*\[\s*([\s\S]*?)\s*\]\s*\)"
        match = re.search(pattern, script_text, re.DOTALL)
        if not match:
            continue
        row_pattern = r"\[\s*'([^']+)'\s*,\s*(-?\d+)\s*,\s*(-?\d+)\s*,\s*(-?\d+)\s*\]"
        for date, enrolled, remaining, registered in re.findall(row_pattern, match.group(1)):
            selection_records.append(
                {
                    "date": date,
                    "enrolled": int(enrolled),
                    "remaining": int(remaining),
                    "registered": int(registered),
                }
            )
        break
    return selection_records


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
        # save_course_info_to_db(course_info_df)
        logger.info(f"[crawl_course] Done! Fetched {len(course_info_df)} courses")

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

        # save_course_detail_to_db(course_detail_df)
        logger.info(f"[crawl_course] Done! Fetched {len(course_detail_df)} courses")

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
    url = f"{BASE_URL}/view/{academic_year}/{academic_semester}/{course_code}/"

    async with semaphore:
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    # 失敗時可以 print，但建議使用 logging 避免干擾進度條
                    # print(f"Failed to fetch {course_code}, status: {response.status}")
                    return None
                html = await response.text()

            soup = BeautifulSoup(html, "html.parser")

            page_text = clean_text(soup.select_one("#content")) or clean_text(soup.body)
            closed_notice = soup.find(class_="warning closable")
            hero_text = clean_text(soup.select_one("#course-hero"))
            is_closed = bool(
                closed_notice
                or "本課程已於" in page_text
                or "停開" in hero_text
            )

            teaching_goal = extract_accordion_section(soup, "教育目標")
            course_description = extract_accordion_section(soup, "課程概述")
            if not course_description:
                course_description = extract_accordion_section(soup, "課程描述")

            return {
                "course_code": course_code,
                "is_closed": is_closed,
                "teachers": extract_teachers(soup),
                "grading_items": extract_grading_items(soup),
                "selection_records": extract_selection_records(soup),
                "teaching_goal": teaching_goal,
                "course_description": course_description,
                "basic_info": extract_hero_basic_info(soup),
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

    concurrency_limit = config.concurrency_limit
    semaphore = asyncio.Semaphore(concurrency_limit)

    async with aiohttp.ClientSession() as session:
        # 使用 progress context manager
        with progress:
            task_id = progress.add_task(
                f"[cyan]fetching course details (concurrency: {concurrency_limit})...",
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
