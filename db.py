import pymongo
import pandas as pd
from dotenv import load_dotenv
import os


load_dotenv()

DB_NAME = os.getenv("DB_NAME")

myclient: pymongo.MongoClient[dict] = pymongo.MongoClient(os.getenv("DB_URI"))


def save_course_schedule_to_db(df: pd.DataFrame) -> None:
    """
    將 course_schedule DataFrame 寫入 MongoDB 資料庫
    """

    assert DB_NAME, "DB_NAME must be set in .env file"

    try:
        print("Saving course schedule to DB...")
        mydb = myclient[DB_NAME]
        collection = mydb["course_schedule"]
        # 清空 collection
        collection.delete_many({})
        # DataFrame 轉 dict 並批次寫入
        records = df.to_dict(orient="records")
        if records:
            collection.insert_many(records)
        print("Success saving course schedule to DB")
    except Exception as e:
        print(f"Error saving course schedule to DB: {e}")


def save_course_info_to_db(df: pd.DataFrame) -> None:
    """
    將 course_info DataFrame 寫入 MongoDB 資料庫
    """

    assert DB_NAME, "DB_NAME must be set in .env file"

    try:
        print("Saving course info to DB...")
        mydb = myclient[DB_NAME]
        collection = mydb["course_info"]
        # 清空 collection
        collection.delete_many({})
        # DataFrame 轉 dict 並批次寫入
        records = df.to_dict(orient="records")
        if records:
            collection.insert_many(records)
        print("Success saving course info to DB")
    except Exception as e:
        print(f"Error saving course info to DB: {e}")


def save_course_detail_to_db(df: pd.DataFrame) -> None:
    """
    將 course_detail DataFrame（已包含巢狀結構）寫入 MongoDB 資料庫
    """

    assert DB_NAME, "DB_NAME must be set in .env file"

    try:
        print("Saving course detail to DB...")
        mydb = myclient[DB_NAME]
        collection = mydb["course_detail"]
        # 清空 collection
        collection.delete_many({})
        
        if df.empty:
            return
            
        documents = []
        
        for _, row in df.iterrows():
            # 處理評分項目的百分比轉換
            grading_items = []
            if isinstance(row['grading_items'], list):
                for item in row['grading_items']:
                    processed_item = {
                        "method": item.get('method', ''),
                        "percentage": int(item.get('percentage', 0)) if str(item.get('percentage', '')).isdigit() else item.get('percentage', ''),
                        "description": item.get('description', '')
                    }
                    grading_items.append(processed_item)
            
            document = {
                "course_code": row['course_code'],
                "teachers": row['teachers'] if isinstance(row['teachers'], list) else [],
                "grading_items": grading_items,
                "selection_records": row['selection_records'] if isinstance(row['selection_records'], list) else [],
                "teaching_goal": row['teaching_goal'] if pd.notna(row['teaching_goal']) else "",
                "course_description": row['course_description'] if pd.notna(row['course_description']) else "",
                "basic_info": row['basic_info'] if isinstance(row['basic_info'], dict) else {}
            }
            documents.append(document)
        
        if documents:
            collection.insert_many(documents)
            print("Success saving course detail to DB")
    
    except Exception as e:
        print(f"Error saving course detail to DB: {e}")
