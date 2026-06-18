import io
import logging
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import config
from db import save_department_categories_to_db, save_departments_to_db

from utils.logger import setup_logger, get_logger

setup_logger()
logger = get_logger(__name__)

BASE_URL = "https://course.thu.edu.tw"


def get_department_term() -> tuple[str, str]:
    """Use the latest configured academic term for global department metadata."""
    return max(config.academic_terms, key=lambda term: (int(term[0]), int(term[1])))


def clean_text(element) -> str:
    if not element:
        return ""
    return re.sub(r"\s+", " ", element.get_text(" ", strip=True)).strip()


def extract_dept_code(href: str | None) -> str:
    if not href:
        return ""
    return href.strip("/").split("/")[-1]


def fetch_course_info_df(
    session: requests.Session, academic_year: str, academic_semester: str
) -> pd.DataFrame:
    url = f"{BASE_URL}/opendatadownload/list/{academic_year}/{academic_semester}/"
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return pd.read_csv(
        io.StringIO(response.text),
        dtype={"選課代碼": str, "開課系所代碼": str},
        on_bad_lines="skip",
    )


def fetch_college_department_map(
    session: requests.Session,
    academic_year: str,
    academic_semester: str,
    category_code: str,
) -> dict[str, str]:
    """Read department links from the redesigned DataTables course API."""
    try:
        response = session.get(
            f"{BASE_URL}/api/course-list",
            params={
                "year": academic_year,
                "term": academic_semester,
                "college": category_code,
                "draw": 1,
                "start": 0,
                "length": 5000,
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as e:
        logger.warning(
            f"[fetch_dept_categories] Could not fetch college API for {category_code}: {e}"
        )
        return {}

    dept_map: dict[str, str] = {}
    for row in payload.get("data", []):
        if not isinstance(row, list) or len(row) < 7:
            continue
        memo_soup = BeautifulSoup(str(row[6]), "html.parser")
        for link in memo_soup.find_all("a", href=True):
            href = link.get("href", "")
            pattern = rf"/view-dept/{academic_year}/{academic_semester}/([^/]+)/?"
            match = re.search(pattern, href)
            if not match:
                continue
            dept_code = match.group(1)
            dept_name = clean_text(link)
            if dept_code and dept_name:
                dept_map[dept_code] = dept_name
    return dept_map



def main() -> None:
    """獲取系所分類和系所資料"""
    logger.info("[crawl_departments] Starting departments crawler")

    try:
        categories_df, departments_df = fetch_dept_categories()

        # 儲存到資料庫
        if not categories_df.empty:
            save_department_categories_to_db(categories_df)

        if not departments_df.empty:
            save_departments_to_db(departments_df)

    except Exception as e:
        logger.error(f"[crawl_departments] Departments crawler failed: {e}")

    logger.info("[crawl_departments] Departments crawler task completed")


def fetch_dept_categories() -> tuple[pd.DataFrame, pd.DataFrame]:
    """獲取所有系所分類和系所資訊"""
    try:
        academic_year, academic_semester = get_department_term()
        session = requests.Session()
        response = session.get(
            f"{BASE_URL}/view-dept/{academic_year}/{academic_semester}/",
            timeout=30,
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        categories_data = []
        category_dept_lookup: dict[str, dict[str, str]] = {}

        dept_categories = soup.select("#dept-nav-colleges a[href]")
        logger.info(f"[fetch_dept_categories] Found {len(dept_categories)} categories")

        for category in dept_categories:
            category_name = clean_text(category.select_one(".flex-fill")) or clean_text(category)
            category_href = category.get("href")
            if not category_href:
                continue

            category_code = extract_dept_code(category_href)
            if not category_code:
                continue

            categories_data.append(
                {
                    "category_code": category_code,
                    "category_name": category_name,
                    "category_url": f"{BASE_URL}{category_href}",
                    "category_href": category_href,
                }
            )
            logger.info(
                f"[fetch_dept_categories] Found category: {category_name} (code: {category_code})"
            )
            category_dept_lookup[category_code] = fetch_college_department_map(
                session,
                academic_year,
                academic_semester,
                category_code,
            )

        course_info_df = fetch_course_info_df(
            session, academic_year, academic_semester
        )
        if course_info_df.empty:
            logger.warning("[fetch_dept_categories] Course info CSV is empty")
            return pd.DataFrame(categories_data), pd.DataFrame()

        departments_source = (
            course_info_df[["開課系所代碼", "開課系所名稱"]]
            .dropna()
            .drop_duplicates()
            .sort_values("開課系所代碼")
        )

        category_names = {
            category["category_code"]: category["category_name"]
            for category in categories_data
        }
        category_by_dept: dict[str, tuple[str, str]] = {}
        for category_code, dept_map in category_dept_lookup.items():
            for dept_code in dept_map:
                category_by_dept.setdefault(
                    dept_code, (category_code, category_names.get(category_code, ""))
                )

        departments_data = []
        used_uncategorized = False
        for _, row in departments_source.iterrows():
            department_code = str(row["開課系所代碼"]).strip()
            department_name = str(row["開課系所名稱"]).strip()
            category_code, category_name = category_by_dept.get(
                department_code, ("uncategorized", "未分類")
            )
            if category_code == "uncategorized":
                used_uncategorized = True

            department_href = (
                f"/view-dept/{academic_year}/{academic_semester}/{department_code}/"
            )
            departments_data.append(
                {
                    "category_code": category_code,
                    "category_name": category_name,
                    "department_code": department_code,
                    "department_name": department_name,
                    "department_url": f"{BASE_URL}{department_href}",
                    "department_href": department_href,
                }
            )

        if used_uncategorized and not any(
            category["category_code"] == "uncategorized" for category in categories_data
        ):
            category_href = f"/view-dept/{academic_year}/{academic_semester}/"
            categories_data.append(
                {
                    "category_code": "uncategorized",
                    "category_name": "未分類",
                    "category_url": f"{BASE_URL}{category_href}",
                    "category_href": category_href,
                }
            )

        categories_df = pd.DataFrame(categories_data)
        departments_df = pd.DataFrame(departments_data)

        logger.info(f"[fetch_dept_categories] Total categories: {len(categories_df)}")
        logger.info(f"[fetch_dept_categories] Total departments: {len(departments_df)}")

        # 顯示結果
        logger.info("\n=== 系所分類 ===")
        logger.info(categories_df)
        logger.info("\n=== 系所列表 ===")
        logger.info(departments_df)

        # 儲存為 CSV (可選)
        if config.db_env == "dev":
            categories_df.to_csv(
                "department_categories.csv", index=False, encoding="utf-8-sig"
            )
            departments_df.to_csv("departments.csv", index=False, encoding="utf-8-sig")
            logger.info("[fetch_dept_categories] Data saved to CSV files")

        return categories_df, departments_df

    except Exception as e:
        logger.error(f"[fetch_dept_categories] Departments crawler failed: {e}")
        return pd.DataFrame(), pd.DataFrame()


def process_departments_df(departments_df: pd.DataFrame) -> pd.DataFrame:
    """處理系所資料"""
    if departments_df.empty:
        return departments_df

    # 可以在這裡添加額外的資料處理邏輯
    # 例如：提取課程數量、清理資料等

    return departments_df


if __name__ == "__main__":
    main()
