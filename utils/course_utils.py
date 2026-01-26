import io
import logging
from utils.logger import get_logger

logger = get_logger(__name__)

import pandas as pd
import requests

from utils.dataframe_utils import process_course_info_df


def fetch_course_info(academic_year: str, academic_semester: str) -> pd.DataFrame:
    """獲取課程資訊"""
    try:
        response = requests.get(
            f"https://course.thu.edu.tw/opendatadownload/list/{academic_year}/{academic_semester}/"
        )
        df = pd.read_csv(
            io.StringIO(response.text), dtype={"選課代碼": str, "開課系所代碼": str}
        )
        return df
    except Exception as e:
        logger.error(f"Error fetching course info: {e}")
        return pd.DataFrame()


def get_course_codes(academic_year: str, academic_semester: str) -> list[str]:
    """獲取課程代碼列表"""
    try:
        course_info_df = fetch_course_info(academic_year, academic_semester)
        course_info_df = process_course_info_df(course_info_df)
        return course_info_df["course_code"].tolist()
    except Exception as e:
        logger.error(f"Error getting course codes: {e}")
        return []
