import pymongo
import pandas as pd
from dotenv import load_dotenv
import os


load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_ENV = os.getenv("DB_ENV", "prod")  # 預設為生產環境

myclient: pymongo.MongoClient[dict] = pymongo.MongoClient(os.getenv("DB_URI"))


def get_collection_name(base_name: str) -> str:
    """根據環境變數返回資料表名稱"""
    if DB_ENV == "dev":
        return f"{base_name}_dev"
    return base_name


def save_course_schedule_to_db(df: pd.DataFrame) -> None:
    """
    將 course_schedule DataFrame 寫入 MongoDB 資料庫
    """

    assert DB_NAME, "DB_NAME must be set in .env file"

    try:
        collection_name = get_collection_name("course_schedule")
        print(f"Saving course schedule to DB (collection: {collection_name})...")
        mydb = myclient[DB_NAME]
        collection = mydb[collection_name]
        # 清空 collection
        collection.delete_many({})
        # DataFrame 轉 dict 並批次寫入
        records = df.to_dict(orient="records")
        if records:
            collection.insert_many(records)
        print(f"Success saving course schedule to DB (collection: {collection_name})")
    except Exception as e:
        print(f"Error saving course schedule to DB: {e}")


def save_course_info_to_db(df: pd.DataFrame) -> None:
    """
    將 course_info DataFrame 寫入 MongoDB 資料庫
    """

    assert DB_NAME, "DB_NAME must be set in .env file"

    try:
        collection_name = get_collection_name("course_info")
        print(f"Saving course info to DB (collection: {collection_name})...")
        mydb = myclient[DB_NAME]
        collection = mydb[collection_name]
        # 清空 collection
        collection.delete_many({})
        # DataFrame 轉 dict 並批次寫入
        records = df.to_dict(orient="records")
        if records:
            collection.insert_many(records)
        print(f"Success saving course info to DB (collection: {collection_name})")
    except Exception as e:
        print(f"Error saving course info to DB: {e}")


def save_course_detail_to_db(df: pd.DataFrame) -> None:
    """
    將 course_detail DataFrame（已包含巢狀結構）寫入 MongoDB 資料庫
    """

    assert DB_NAME, "DB_NAME must be set in .env file"

    try:
        collection_name = get_collection_name("course_detail")
        print(f"Saving course detail to DB (collection: {collection_name})...")
        mydb = myclient[DB_NAME]
        collection = mydb[collection_name]
        # 清空 collection
        collection.delete_many({})

        if df.empty:
            return

        documents = []

        for _, row in df.iterrows():
            # 處理評分項目的百分比轉換
            grading_items = []
            if isinstance(row["grading_items"], list):
                for item in row["grading_items"]:
                    processed_item = {
                        "method": item.get("method", ""),
                        "percentage": (
                            int(item.get("percentage", 0))
                            if str(item.get("percentage", "")).isdigit()
                            else item.get("percentage", "")
                        ),
                        "description": item.get("description", ""),
                    }
                    grading_items.append(processed_item)

            document = {
                "course_code": row["course_code"],
                "is_closed": row.get("is_closed", False),
                "teachers": (
                    row["teachers"] if isinstance(row["teachers"], list) else []
                ),
                "grading_items": grading_items,
                "selection_records": (
                    row["selection_records"]
                    if isinstance(row["selection_records"], list)
                    else []
                ),
                "teaching_goal": (
                    row["teaching_goal"] if pd.notna(row["teaching_goal"]) else ""
                ),
                "course_description": (
                    row["course_description"]
                    if pd.notna(row["course_description"])
                    else ""
                ),
                "basic_info": (
                    row["basic_info"] if isinstance(row["basic_info"], dict) else {}
                ),
            }
            documents.append(document)

        if documents:
            collection.insert_many(documents)
        print(f"Success saving course detail to DB (collection: {collection_name})")
    except Exception as e:
        print(f"Error saving course detail to DB: {e}")


def get_course_codes_from_db() -> list[str]:
    """從資料庫獲取課程代碼列表"""
    try:
        collection_name = get_collection_name("course_info")
        mydb = myclient[DB_NAME]
        collection = mydb[collection_name]

        # 獲取所有課程代碼
        course_codes = collection.distinct("course_code")
        return course_codes
    except Exception as e:
        print(f"Error getting course codes from DB: {e}")
        return []
