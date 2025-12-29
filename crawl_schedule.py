import logging

import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from db import save_course_schedule_to_db
from utils.dataframe_utils import process_course_schedule_df

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """獲取選課時間表"""
    logger.info("[crawl_schedule] Starting course schedule crawler")

    try:
        course_schedule_df = fetch_course_selection_schedule()
        course_schedule_df = process_course_schedule_df(course_schedule_df)
        save_course_schedule_to_db(course_schedule_df)
    except Exception as e:
        logger.error(f"[crawl_schedule] Course schedule crawler failed: {e}")

    logger.info("[crawl_schedule] Course schedule crawler task completed")


def fetch_course_selection_schedule() -> pd.DataFrame:
    try:
        response = requests.get("https://course.thu.edu.tw/index")
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table")

        if not isinstance(table, Tag):
            raise TypeError(
                "Expected a Tag object for table, got {}".format(type(table))
            )

        rows = table.find_all("tr")
        data: list[list[str]] = []
        for row in rows:
            if isinstance(row, Tag):
                cols = [col.get_text(strip=True) for col in row.find_all(["td", "th"])]
                if cols:
                    data.append(cols)

        # 第一列為欄位名稱
        if data:
            df = pd.DataFrame(data[1:], columns=data[0])
        else:
            df = pd.DataFrame()
        return df
    except Exception as e:
        logger.error(f"Error fetching course selection schedule: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    main()
