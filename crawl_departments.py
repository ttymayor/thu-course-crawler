import logging

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import config
from db import save_department_categories_to_db, save_departments_to_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)




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
        base_url = "https://course.thu.edu.tw"
        response = requests.get(
            f"{base_url}/view-dept/{config.academic_year}/{config.academic_semester}/"
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        dept_category_list = soup.find(class_="side_bar_menu")

        if not dept_category_list:
            logger.error("[fetch_dept_categories] Could not find side_bar_menu")
            return pd.DataFrame(), pd.DataFrame()

        dept_categories = dept_category_list.find_all("a")

        categories_data = []
        departments_data = []
        everything_category = None

        logger.info(f"[fetch_dept_categories] Found {len(dept_categories)} categories")

        # 第一階段：處理一般分類
        for category in dept_categories:
            category_name = category.text.strip()
            category_href = category.get("href")

            if not category_href:
                continue

            # 如果是 everything，先儲存起來，稍後處理
            if (
                category_href
                == f"/view-dept/{config.academic_year}/{config.academic_semester}/everything"
            ):
                everything_category = category
                continue

            # 從 URL 提取 category_code
            # URL 格式: /view-dept/114/1/{category_code}
            category_code = category_href.split("/")[-1]

            # 儲存分類資訊
            category_info = {
                "category_code": category_code,
                "category_name": category_name,
                "category_url": f"{base_url}{category_href}",
                "category_href": category_href,
            }
            categories_data.append(category_info)

            logger.info(
                f"[fetch_dept_categories] Processing category: {category_name} (code: {category_code})"
            )

            # 獲取該分類下的所有系所
            try:
                category_url = f"{base_url}{category_href}"
                category_response = requests.get(category_url)
                category_response.raise_for_status()
                category_soup = BeautifulSoup(category_response.text, "html.parser")

                # 查找系所表格
                dept_table = category_soup.find("table")

                if dept_table:
                    dept_rows = dept_table.find("tbody")
                    if dept_rows:
                        dept_rows = dept_rows.find_all("tr")
                    else:
                        dept_rows = dept_table.find_all("tr")[1:]  # 跳過表頭

                    for row in dept_rows:
                        cells = row.find_all("td")
                        if len(cells) >= 2:
                            # 第一個 cell 包含系所名稱和連結
                            dept_link = cells[0].find("a")
                            if dept_link:
                                dept_name = dept_link.text.strip()
                                dept_href = dept_link.get("href")

                                # 從 URL 提取 department_code
                                # URL 格式: /view-dept/114/1/{department_code}
                                department_code = dept_href.split("/")[-1]

                                # 第二個 cell 包含課程數量資訊
                                # course_info = cells[1].text.strip()

                                dept_info = {
                                    "category_code": category_code,
                                    "category_name": category_name,
                                    "department_code": department_code,
                                    "department_name": dept_name,
                                    "department_url": f"{base_url}{dept_href}",
                                    "department_href": dept_href,
                                    # "course_info": course_info
                                }
                                departments_data.append(dept_info)
                                logger.info(
                                    f"  - Found department: {dept_name} (code: {department_code})"
                                )

            except Exception as e:
                logger.error(
                    f"[fetch_dept_categories] Error processing category {category_name}: {e}"
                )
                continue

        # 第二階段：處理 everything 分類，找出未分類的系所
        if everything_category:
            logger.info(
                "[fetch_dept_categories] Processing 'everything' category for uncategorized departments"
            )

            # 收集已經分類的系所代碼
            categorized_dept_codes = set(
                dept["department_code"] for dept in departments_data
            )

            try:
                everything_href = everything_category.get("href")
                everything_url = f"{base_url}{everything_href}"
                everything_response = requests.get(everything_url)
                everything_response.raise_for_status()
                everything_soup = BeautifulSoup(everything_response.text, "html.parser")

                # 查找系所表格
                dept_table = everything_soup.find("table")

                if dept_table:
                    dept_rows = dept_table.find("tbody")
                    if dept_rows:
                        dept_rows = dept_rows.find_all("tr")
                    else:
                        dept_rows = dept_table.find_all("tr")[1:]  # 跳過表頭

                    uncategorized_depts = []
                    for row in dept_rows:
                        cells = row.find_all("td")
                        if len(cells) >= 2:
                            dept_link = cells[0].find("a")
                            if dept_link:
                                dept_name = dept_link.text.strip()
                                dept_href = dept_link.get("href")
                                department_code = dept_href.split("/")[-1]

                                # 檢查是否已經在其他分類中
                                if department_code not in categorized_dept_codes:
                                    dept_info = {
                                        "category_code": "uncategorized",
                                        "category_name": "未分類",
                                        "department_code": department_code,
                                        "department_name": dept_name,
                                        "department_url": f"{base_url}{dept_href}",
                                        "department_href": dept_href,
                                    }
                                    uncategorized_depts.append(dept_info)
                                    logger.info(
                                        f"  - Found uncategorized department: {dept_name} (code: {department_code})"
                                    )

                    # 如果有未分類的系所，新增「未分類」分類
                    if uncategorized_depts:
                        uncategorized_category = {
                            "category_code": "uncategorized",
                            "category_name": "未分類",
                            "category_url": f"{base_url}/view-dept/{config.academic_year}/{config.academic_semester}/everything",
                            "category_href": f"/view-dept/{config.academic_year}/{config.academic_semester}/everything",
                        }
                        categories_data.append(uncategorized_category)
                        departments_data.extend(uncategorized_depts)
                        logger.info(
                            f"[fetch_dept_categories] Added {len(uncategorized_depts)} uncategorized departments"
                        )

            except Exception as e:
                logger.error(
                    f"[fetch_dept_categories] Error processing 'everything' category: {e}"
                )

        # 建立 DataFrames
        categories_df = pd.DataFrame(categories_data)
        departments_df = pd.DataFrame(departments_data)

        logger.info(f"[fetch_dept_categories] Total categories: {len(categories_df)}")
        logger.info(f"[fetch_dept_categories] Total departments: {len(departments_df)}")

        # 顯示結果
        print("\n=== 系所分類 ===")
        print(categories_df)
        print("\n=== 系所列表 ===")
        print(departments_df)

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
