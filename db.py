import logging
import math
import os

import pandas as pd
import pymongo
from dotenv import load_dotenv
from pymongo import UpdateOne

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_ENV = os.getenv("DB_ENV", "prod")

myclient: pymongo.MongoClient[dict] = pymongo.MongoClient(os.getenv("DB_URI"))


def get_collection_name(base_name: str) -> str:
    """根據環境變數返回資料表名稱"""
    if DB_ENV == "dev":
        return f"{base_name}_dev"
    return base_name


def save_merged_courses_to_db(df: pd.DataFrame) -> None:
    """
    將合併後的完整課程資料 (Info + Detail) 寫入 MongoDB
    資料表名稱預設為: courses (或 courses_dev)
    """
    if df.empty:
        logger.error("Merged course DataFrame is empty")
        return

    assert DB_NAME, "DB_NAME must be set in .env file"

    try:
        # 使用新的集合名稱，例如 'courses' 來存放完整的資料
        collection_name = get_collection_name("courses")
        logger.info(f"Saving merged courses to DB (collection: {collection_name})...")

        mydb = myclient[DB_NAME]
        collection = mydb[collection_name]

        # 建立索引
        try:
            collection.create_index("course_code", unique=True)
        except pymongo.errors.OperationFailure as e:
            if e.code == 86:  # IndexKeySpecsConflict
                logger.warning(
                    f"Index conflict detected in {collection_name}. Dropping existing index 'course_code_1' and recreating."
                )
                collection.drop_index("course_code_1")
                collection.create_index("course_code", unique=True)
            else:
                raise e

        ops = []
        # 將 DataFrame 轉為 dict 列表，逐筆處理
        records = df.to_dict(orient="records")

        for row in records:
            # 1. 處理 grading_items (巢狀結構清理)
            raw_grading = row.get("grading_items")
            grading_items = []
            if isinstance(raw_grading, list):
                for item in raw_grading:
                    percentage = item.get("percentage", "")
                    # 嘗試將百分比轉為數字，保留原始邏輯
                    if str(percentage).isdigit():
                        percentage = int(percentage)

                    grading_items.append(
                        {
                            "method": item.get("method", ""),
                            "percentage": percentage,
                            "description": item.get("description", ""),
                        }
                    )

            # 2. 處理 teachers (確保是 list)
            raw_teachers = row.get("teachers")
            teachers = raw_teachers if isinstance(raw_teachers, list) else []

            # 3. 處理 selection_records
            raw_selection = row.get("selection_records")
            selection_records = raw_selection if isinstance(raw_selection, list) else []

            # 4. 處理 basic_info (確保是 dict)
            raw_basic = row.get("basic_info")
            basic_info = raw_basic if isinstance(raw_basic, dict) else {}

            # 5. 處理其他可能為 NaN 的欄位 (因為 Left Join 可能產生 NaN)
            def clean_nan(val, default):
                # 檢查是否為 float('nan') 或 None
                if val is None:
                    return default
                if isinstance(val, float) and math.isnan(val):
                    return default
                return val

            # 建構最終要寫入的 Document
            # 先複製所有欄位，然後覆蓋掉處理過的複雜欄位
            document = row.copy()

            # 覆蓋處理過的欄位
            document["grading_items"] = grading_items
            document["teachers"] = teachers
            document["selection_records"] = selection_records
            document["basic_info"] = basic_info

            # 清理其他關鍵欄位
            document["is_closed"] = clean_nan(row.get("is_closed"), False)
            document["teaching_goal"] = clean_nan(row.get("teaching_goal"), "")
            document["course_description"] = clean_nan(
                row.get("course_description"), ""
            )

            # 加入批次操作
            ops.append(
                UpdateOne(
                    {"course_code": row["course_code"]}, {"$set": document}, upsert=True
                )
            )

        # 執行批次寫入
        if ops:
            result = collection.bulk_write(ops)
            logger.info(
                f"Write Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted: {result.upserted_count}"
            )

        logger.info(
            f"Success saving merged courses to DB (collection: {collection_name})"
        )

    except Exception as e:
        logger.error(f"Error saving merged courses to DB: {e}")
        # 在開發環境印出詳細錯誤以便除錯
        import traceback

        traceback.print_exc()


def save_course_schedule_to_db(df: pd.DataFrame) -> None:
    """
    將 course_schedule DataFrame 寫入 MongoDB 資料庫
    """

    assert DB_NAME, "DB_NAME must be set in .env file"

    try:
        collection_name = get_collection_name("course_schedule")
        logger.info(f"Saving course schedule to DB (collection: {collection_name})...")
        mydb = myclient[DB_NAME]
        collection = mydb[collection_name]

        collection.create_index("id")

        if df.empty:
            logger.warning("Course schedule DataFrame is empty, skipping save.")
            return

        # 刪除舊資料
        collection.delete_many({})

        # 新增新資料
        records = df.to_dict(orient="records")
        if records:
            collection.insert_many(records)

        logger.info(
            f"Success saving course schedule to DB (collection: {collection_name})"
        )
    except Exception as e:
        logger.error(f"Error saving course schedule to DB: {e}")


def save_course_info_to_db(df: pd.DataFrame) -> None:
    """
    將 course_info DataFrame 寫入 MongoDB 資料庫
    """

    if df.empty:
        logger.info("Course info DataFrame is empty")
        return

    assert DB_NAME, "DB_NAME must be set in .env file"

    try:
        collection_name = get_collection_name("course_info")
        logger.info(f"Saving course info to DB (collection: {collection_name})...")
        mydb = myclient[DB_NAME]
        collection = mydb[collection_name]
        # 創建索引
        try:
            collection.create_index("course_code", unique=True)
        except pymongo.errors.OperationFailure as e:
            if e.code == 86:  # IndexKeySpecsConflict
                logger.warning(
                    f"Index conflict detected in {collection_name}. Dropping existing index 'course_code_1' and recreating."
                )
                collection.drop_index("course_code_1")
                collection.create_index("course_code", unique=True)
            else:
                raise e

        ops = []
        records = df.to_dict(orient="records")

        for record in records:
            course_code = record["course_code"]
            ops.append(
                UpdateOne({"course_code": course_code}, {"$set": record}, upsert=True)
            )

        if ops:
            collection.bulk_write(ops)
        logger.info(f"Success saving course info to DB (collection: {collection_name})")
    except Exception as e:
        logger.error(f"Error saving course info to DB: {e}")


def save_course_detail_to_db(df: pd.DataFrame) -> None:
    """
    將 course_detail DataFrame（已包含巢狀結構）寫入 MongoDB 資料庫
    """

    if df.empty:
        logger.info("Course detail DataFrame is empty")
        return

    assert DB_NAME, "DB_NAME must be set in .env file"

    try:
        collection_name = get_collection_name("course_detail")
        logger.info(f"Saving course detail to DB (collection: {collection_name})...")
        mydb = myclient[DB_NAME]
        collection = mydb[collection_name]

        try:
            collection.create_index("course_code", unique=True)
        except pymongo.errors.OperationFailure as e:
            if e.code == 86:  # IndexKeySpecsConflict
                logger.warning(
                    f"Index conflict detected in {collection_name}. Dropping existing index 'course_code_1' and recreating."
                )
                collection.drop_index("course_code_1")
                collection.create_index("course_code", unique=True)
            else:
                raise e

        for _, row in df.iterrows():
            grading_items = []
            if isinstance(row["grading_items"], list):
                for item in row["grading_items"]:
                    grading_items.append(
                        {
                            "method": item.get("method", ""),
                            "percentage": (
                                int(item.get("percentage"))
                                if str(item.get("percentage", "")).isdigit()
                                else item.get("percentage", "")
                            ),
                            "description": item.get("description", ""),
                        }
                    )

            document = {
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

            collection.update_one(
                {"course_code": row["course_code"]},  # 唯一鍵
                {"$set": document},
                upsert=True,
            )

        logger.info(
            f"Success saving course detail to DB (collection: {collection_name})"
        )
    except Exception as e:
        logger.error(f"Error saving course detail to DB: {e}")


def save_department_categories_to_db(df: pd.DataFrame) -> None:
    """
    將 department_categories DataFrame 寫入 MongoDB 資料庫
    """
    if df.empty:
        logger.info("Department categories DataFrame is empty")
        return

    assert DB_NAME, "DB_NAME must be set in .env file"

    try:
        collection_name = get_collection_name("department_categories")
        logger.info(
            f"Saving department categories to DB (collection: {collection_name})..."
        )
        mydb = myclient[DB_NAME]
        collection = mydb[collection_name]

        # 建立索引
        try:
            collection.create_index("category_code", unique=True)
        except pymongo.errors.OperationFailure as e:
            if e.code == 86:  # IndexKeySpecsConflict
                logger.warning(
                    f"Index conflict detected in {collection_name}. Dropping existing index 'category_code_1' and recreating."
                )
                collection.drop_index("category_code_1")
                collection.create_index("category_code", unique=True)
            else:
                raise e

        ops = []
        records = df.to_dict(orient="records")

        for record in records:
            ops.append(
                UpdateOne(
                    {"category_code": record["category_code"]},
                    {"$set": record},
                    upsert=True,
                )
            )

        if ops:
            result = collection.bulk_write(ops)
            logger.info(
                f"Write Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted: {result.upserted_count}"
            )

        logger.info(
            f"Success saving department categories to DB (collection: {collection_name})"
        )
    except Exception as e:
        logger.error(f"Error saving department categories to DB: {e}")
        import traceback

        traceback.print_exc()


def save_departments_to_db(df: pd.DataFrame) -> None:
    """
    將 departments DataFrame 寫入 MongoDB 資料庫
    """
    if df.empty:
        logger.info("Departments DataFrame is empty")
        return

    assert DB_NAME, "DB_NAME must be set in .env file"

    try:
        collection_name = get_collection_name("departments")
        logger.info(f"Saving departments to DB (collection: {collection_name})...")
        mydb = myclient[DB_NAME]
        collection = mydb[collection_name]

        # 建立索引
        try:
            collection.create_index("department_code", unique=True)
        except pymongo.errors.OperationFailure as e:
            if e.code == 86:  # IndexKeySpecsConflict
                logger.warning(
                    f"Index conflict detected in {collection_name}. Dropping existing index 'department_code_1' and recreating."
                )
                collection.drop_index("department_code_1")
                collection.create_index("department_code", unique=True)
            else:
                raise e
        collection.create_index("category_code")  # 方便按分類查詢

        ops = []
        records = df.to_dict(orient="records")

        for record in records:
            ops.append(
                UpdateOne(
                    {"department_code": record["department_code"]},
                    {"$set": record},
                    upsert=True,
                )
            )

        if ops:
            result = collection.bulk_write(ops)
            logger.info(
                f"Write Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted: {result.upserted_count}"
            )

        logger.info(f"Success saving departments to DB (collection: {collection_name})")
    except Exception as e:
        logger.error(f"Error saving departments to DB: {e}")
        import traceback

        traceback.print_exc()
