import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
import io
import pandas as pd
from utils.dataframe_time_utils import process_course_schedule_df, process_course_info_df
from db import save_course_schedule_to_db, save_course_info_to_db


def main() -> None:
    # 獲取選課時間表
    course_url = "https://course.thu.edu.tw/index"
    course_schedule_df = fetch_course_selection_schedule(course_url)
    course_schedule_df = process_course_schedule_df(course_schedule_df)

    try:
        save_course_schedule_to_db(course_schedule_df)
        print('Success saving course schedule to DB')
    except Exception as e:
        print(f"Error saving course schedule to DB: {e}")

    # 獲取課程資訊
    academic_year = "114"
    academic_semester = "1"
    course_info_df = fetch_course_info(academic_year, academic_semester)
    course_info_df = process_course_info_df(course_info_df)

    try:
        save_course_info_to_db(course_info_df)
        print('Success saving course info to DB')
    except Exception as e:
        print(f"Error saving course info to DB: {e}")


def fetch_course_selection_schedule(course_url: str) -> pd.DataFrame:
    try: 
        response = requests.get(course_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table')

        if not isinstance(table, Tag):
            raise TypeError("Expected a Tag object for table, got {}".format(type(table)))

        rows = table.find_all('tr')
        data: list[list[str]] = []
        for row in rows:
            if isinstance(row, Tag):
                cols = [col.get_text(strip=True) for col in row.find_all(['td', 'th'])]
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


def fetch_course_info(academic_year: str, semester: str) -> pd.DataFrame:
    try:
        response = requests.get(f"https://course.thu.edu.tw/opendatadownload/list/{academic_year}/{semester}/")
        df = pd.read_csv(io.StringIO(response.text), dtype={"選課代碼": str, "開課系所代碼": str})
        return df
    except Exception as e:
        print(f"Error fetching course info: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    main()
