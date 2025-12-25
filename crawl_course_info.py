import io
import os
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
from dotenv import load_dotenv

# 假設你的 db 模組原本是分開存的，若要存合併後的表，你可能需要寫一個新的 save function
# from db import save_merged_course_to_db
from db import (
    save_course_detail_to_db,
    save_course_info_to_db,
    save_merged_courses_to_db,
)
from utils.dataframe_time_utils import process_course_info_df

load_dotenv()

DB_ENV = os.getenv("DB_ENV", "prod")
ACADEMIC_YEAR = os.getenv("ACADEMIC_YEAR", "114")
ACADEMIC_SEMESTER = os.getenv("ACADEMIC_SEMESTER", "1")
DEV_DATA_LIMIT = int(os.getenv("DEV_DATA_LIMIT", "10"))


def main() -> None:
    """獲取課程資訊和詳細資訊並整合為一張表"""
    print(f"開始執行課程爬蟲 - {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # --- 1. 爬取課程基本資訊 ---
        print("1. 爬取課程基本資訊...")
        course_info_df = fetch_course_info(ACADEMIC_YEAR, ACADEMIC_SEMESTER)
        course_info_df = process_course_info_df(course_info_df)

        # 這裡依舊可以選擇先存基本資訊，或者等合併後一次存
        save_course_info_to_db(course_info_df)
        print(f"   [OK] 課程基本資訊爬取完成，共 {len(course_info_df)} 個課程")

        # --- 2. 爬取課程詳細資訊 ---
        print("2. 爬取課程詳細資訊...")
        course_codes = course_info_df["course_code"].tolist()

        # 開發模式下限制筆數
        if DB_ENV == "dev":
            course_codes = course_codes[:DEV_DATA_LIMIT]
            print(f"   [DEV MODE] 僅爬取前 {DEV_DATA_LIMIT} 筆詳細資料")

        course_detail_df = fetch_course_detail(
            ACADEMIC_YEAR, ACADEMIC_SEMESTER, course_codes
        )
        # 這裡依舊維持你原本的儲存邏輯
        save_course_detail_to_db(course_detail_df)
        print(f"   [OK] 課程詳細資訊爬取完成，共 {len(course_detail_df)} 個課程")

        # --- 3. 資料整併 (Merge) ---
        print("3. 整合兩張資料表...")

        # 使用 left join: 保留所有 course_info 的資料，將 detail 對應上去
        # 如果該課程沒有 detail (例如 dev 模式沒爬到，或是爬取失敗)，欄位會是 NaN
        merged_df = pd.merge(
            course_info_df, course_detail_df, on="course_code", how="left"
        )

        print(f"   [OK] 資料整合完成，總欄位數: {len(merged_df.columns)}")
        print(f"   整合後資料範例:\n{merged_df.head(1)}")

        save_merged_courses_to_db(merged_df)

        print("課程爬蟲任務完成")

    except Exception as e:
        print(f"課程爬蟲失敗: {e}")
        # 在開發時印出完整的 traceback 會更有幫助
        import traceback

        traceback.print_exc()

    print(f"課程爬蟲任務完成 - {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")


def fetch_course_info(academic_year: str, academic_semester: str) -> pd.DataFrame:
    # ... (保持原本的程式碼)
    """獲取課程基本資訊"""
    try:
        response = requests.get(
            f"https://course.thu.edu.tw/opendatadownload/list/{academic_year}/{academic_semester}/"
        )
        df = pd.read_csv(
            io.StringIO(response.text), dtype={"選課代碼": str, "開課系所代碼": str}
        )
        return df
    except Exception as e:
        print(f"Error fetching course info: {e}")
        return pd.DataFrame()


def fetch_course_detail(
    academic_year: str, academic_semester: str, course_codes: List[str]
) -> pd.DataFrame:
    # ... (保持原本的程式碼，這裡不需要變動)
    """
    獲取課程詳細資訊，包含評分方式和授課教師
    回傳巢狀結構的 DataFrame
    """
    course_details = []
    print(f"Fetching course details for {len(course_codes)} courses...")

    for course_code in course_codes:
        try:
            response = requests.get(
                f"https://course.thu.edu.tw/view/{academic_year}/{academic_semester}/{course_code}/"
            )
            soup = BeautifulSoup(response.text, "html.parser")

            # 找尋是否停開
            closed_notice = soup.find(class_="warning closable")
            if closed_notice:
                course_details.append({"course_code": course_code, "is_closed": True})
                continue

            # 找到第一個表格（評分方式）
            table = soup.find("table")
            if not isinstance(table, Tag):
                print(f"No table found for course {course_code}")
                continue

            rows = table.find_all("tr")
            grading_data = []

            for row in rows:
                if isinstance(row, Tag):
                    cols = [
                        col.get_text(strip=True) for col in row.find_all(["td", "th"])
                    ]
                    if cols:
                        grading_data.append(cols)

            # 獲取評分方式
            grading_items = []
            if grading_data and len(grading_data) > 1:
                for row in grading_data[1:]:  # 跳過標題列
                    if len(row) >= 3:  # 確保有足夠欄位
                        grading_item = {
                            "method": row[0],
                            "percentage": row[1],
                            "description": row[2] if len(row) > 2 else "",
                        }
                        grading_items.append(grading_item)

            # 獲取選課紀錄
            selection_records = []
            # 從 Google Charts script 中提取資料
            scripts = soup.find_all("script")
            for script in scripts:
                if isinstance(script, Tag):
                    script_text = script.get_text()
                    if "google.visualization.arrayToDataTable" in script_text:
                        import re

                        # 尋找 arrayToDataTable 中的資料
                        pattern = r"google\.visualization\.arrayToDataTable\(\s*\[\s*([\s\S]*?)\s*\]\s*\)"
                        match = re.search(pattern, script_text, re.DOTALL)

                        if match:
                            data_content = match.group(1)

                            # 提取每一行資料（跳過標題列）
                            row_pattern = (
                                r"\[\s*'([^']+)',\s*(\d+),\s*(\d+),\s*(\d+)\s*\]"
                            )
                            matches = re.findall(row_pattern, data_content)

                            for match_data in matches:
                                date, enrolled, remaining, registered = match_data
                                selection_record = {
                                    "date": date,
                                    "enrolled": int(enrolled),
                                    "remaining": int(remaining),
                                    "registered": int(registered),
                                }
                                selection_records.append(selection_record)
                            break

            # 獲取授課教師
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

            # 獲取教學目標（從 meta name="description" 取得）
            teaching_goal = None
            try:
                meta_description = soup.find("meta", attrs={"name": "description"})
                # assert meta_description, "Meta description not found" # 建議註解掉 assert，避免中斷迴圈
                if meta_description and hasattr(meta_description, "attrs"):
                    teaching_goal = meta_description.attrs.get("content", "").strip()
                else:
                    # print("Meta description does not have 'attrs' attribute or is None")
                    pass
            except Exception as e:
                print(f"提取 meta description 時出錯: {e}")

            # 獲取課程概述
            course_description: Optional[str] = None
            course_description_element = soup.select_one(
                "#mainContent > div:nth-child(4) > div:nth-child(2) > p:nth-child(2)"
            )
            if course_description_element:
                course_description = course_description_element.get_text(strip=True)

            # 獲取基本資料
            basic_info: Dict[str, Any] = {}
            basic_info_element = soup.select_one(
                "#mainContent > div:nth-child(5) > div:nth-child(2) > div:nth-child(1) > p"
            )
            if basic_info_element:
                # 使用 br 標籤來分割內容
                parts: List[str] = []
                for element in basic_info_element.contents:
                    if isinstance(element, NavigableString):
                        text = element.strip()
                        if text:
                            parts.append(text)
                    elif isinstance(element, Tag) and element.name == "br":
                        # br 標籤作為分隔符號
                        parts.append("\n")

                # 合併所有部分並分行處理
                basic_info_text = "".join(parts)
                lines = [
                    line.strip() for line in basic_info_text.split("\n") if line.strip()
                ]

                # 解析基本資料的各個欄位
                for line in lines:
                    if "：" in line:
                        key, value = line.split("：", 1)
                        key = key.strip()
                        value = value.strip()

                        if key == "選修課" or key == "必修課":
                            basic_info["course_type"] = key
                            # 處理 "必修課，學分數：3-0" 這種格式
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

                # 如果沒有解析到結構化資料，保留原始文字
                if not basic_info:
                    basic_info["raw_text"] = basic_info_text

            # 建立課程詳細資料
            course_detail = {
                "course_code": course_code,
                "is_closed": bool(closed_notice),
                "teachers": teacher_list,
                "grading_items": grading_items,
                "selection_records": selection_records,
                "teaching_goal": teaching_goal,
                "course_description": course_description,
                "basic_info": basic_info,
            }
            course_details.append(course_detail)
            print(f"Success fetching course detail for {course_code}")
        except Exception as e:
            print(f"Error fetching course detail for {course_code}: {e}")
            continue

    return pd.DataFrame(course_details)


if __name__ == "__main__":
    main()
