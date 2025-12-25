import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from db import save_course_schedule_to_db
from utils.dataframe_time_utils import process_course_schedule_df


def main() -> None:
    """獲取選課時間表"""
    print(
        f"開始執行選課時間表爬蟲 - {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        course_schedule_df = fetch_course_selection_schedule()
        course_schedule_df = process_course_schedule_df(course_schedule_df)
        save_course_schedule_to_db(course_schedule_df)
        print("選課時間表爬蟲完成")
    except Exception as e:
        print(f"選課時間表爬蟲失敗: {e}")

    print(
        f"選課時間表爬蟲任務完成 - {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )


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
        print(f"Error fetching course selection schedule: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    main()
