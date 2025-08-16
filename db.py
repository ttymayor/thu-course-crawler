import pymongo
from dotenv import load_dotenv
import os

load_dotenv()

DB_NAME=os.getenv("DB_NAME")

myclient = pymongo.MongoClient(os.getenv("DB_URL"))

def save_course_schedule_to_db(df) -> None:
    """
    將 course_schedule DataFrame 寫入 MongoDB 資料庫
    """

    assert DB_NAME, "DB_NAME must be set in .env file"

    mydb = myclient[DB_NAME]
    collection = mydb["course_schedule"]
    # 清空 collection
    collection.delete_many({})
    # DataFrame 轉 dict 並批次寫入
    records = df.to_dict(orient="records")
    if records:
        collection.insert_many(records)

def save_course_info_to_db(df) -> None:
    """
    將 course_info DataFrame 寫入 MongoDB 資料庫
    """

    assert DB_NAME, "DB_NAME must be set in .env file"

    mydb = myclient[DB_NAME]
    collection = mydb["course_info"]
    # 清空 collection
    collection.delete_many({})
    # DataFrame 轉 dict 並批次寫入
    records = df.to_dict(orient="records")
    if records:
        collection.insert_many(records)
