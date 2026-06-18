import logging
import math
from typing import Any

import pandas as pd
import pymongo
from pymongo import UpdateOne

from config import config

from utils.logger import get_logger

logger = get_logger(__name__)

myclient: pymongo.MongoClient[dict] = pymongo.MongoClient(config.db_uri)

COURSE_TERM_INDEX = [("academic_year", 1), ("academic_semester", 1), ("course_code", 1)]
COURSE_TERM_INDEX_NAME = "academic_term_course_code_unique"
NO_DATA_VALUES = {"", "無資料", "無", "未定", "None", "none", "N/A", "n/a"}


def get_collection_name(base_name: str) -> str:
    """根據環境變數返回資料表名稱"""
    return config.get_collection_name(base_name)


def get_course_term_filter(row: dict) -> dict:
    """Build the compound course identity filter used by course collections."""
    return {
        "academic_year": int(row["academic_year"]),
        "academic_semester": int(row["academic_semester"]),
        "course_code": row["course_code"],
    }


def get_df_term_filter(df: pd.DataFrame) -> dict:
    """Return the term filter for a DataFrame containing one academic term."""
    return {
        "academic_year": int(df["academic_year"].iloc[0]),
        "academic_semester": int(df["academic_semester"].iloc[0]),
    }


def parse_numeric_term(value: Any) -> int | None:
    """Return a normalized academic term value when it is numeric."""
    if value is None:
        return None
    raw_value = str(value).strip()
    if not raw_value.isdigit():
        return None
    return int(raw_value)


def cleanup_course_term_documents(collection) -> None:
    """
    Normalize legacy course term fields before enforcing the compound unique index.

    Older crawls may have stored academic_year / academic_semester as strings.
    MongoDB treats "1" and 1 as different unique-index values, so repeated crawls
    can create logical duplicates. Keep the newest document for each normalized
    term/course identity and rewrite its term fields as integers.
    """
    grouped_docs: dict[tuple[int, int, str], list[dict[str, Any]]] = {}

    for doc in collection.find(
        {
            "academic_year": {"$exists": True},
            "academic_semester": {"$exists": True},
            "course_code": {"$exists": True},
        },
        {"_id": 1, "academic_year": 1, "academic_semester": 1, "course_code": 1},
    ):
        academic_year = parse_numeric_term(doc.get("academic_year"))
        academic_semester = parse_numeric_term(doc.get("academic_semester"))
        course_code = str(doc.get("course_code", "")).strip()
        if academic_year is None or academic_semester is None or not course_code:
            continue

        grouped_docs.setdefault(
            (academic_year, academic_semester, course_code), []
        ).append(doc)

    normalized_count = 0
    duplicate_count = 0

    for (academic_year, academic_semester, _course_code), docs in grouped_docs.items():
        canonical_docs = [
            doc
            for doc in docs
            if doc.get("academic_year") == academic_year
            and doc.get("academic_semester") == academic_semester
        ]
        keep_doc = max(canonical_docs or docs, key=lambda doc: str(doc["_id"]))
        duplicate_ids = [doc["_id"] for doc in docs if doc["_id"] != keep_doc["_id"]]

        if duplicate_ids:
            delete_result = collection.delete_many({"_id": {"$in": duplicate_ids}})
            duplicate_count += delete_result.deleted_count

        if (
            keep_doc.get("academic_year") != academic_year
            or keep_doc.get("academic_semester") != academic_semester
        ):
            collection.update_one(
                {"_id": keep_doc["_id"]},
                {
                    "$set": {
                        "academic_year": academic_year,
                        "academic_semester": academic_semester,
                    }
                },
            )
            normalized_count += 1

    if duplicate_count or normalized_count:
        logger.info(
            f"Normalized course term documents in {collection.name}: "
            f"{normalized_count} updated, {duplicate_count} duplicates removed"
        )


def ensure_course_term_index(collection) -> None:
    """Replace legacy course_code uniqueness with term-aware uniqueness."""
    cleanup_course_term_documents(collection)

    for index_name, index_info in collection.index_information().items():
        if index_name == "_id_":
            continue
        if index_info.get("key") == [("course_code", 1)] and index_info.get("unique"):
            logger.warning(
                f"Dropping legacy unique index '{index_name}' before creating term-aware index."
            )
            collection.drop_index(index_name)

    collection.create_index(
        COURSE_TERM_INDEX,
        unique=True,
        name=COURSE_TERM_INDEX_NAME,
    )


def normalize_no_data_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    value = str(value).strip()
    return "" if value in NO_DATA_VALUES else value


def normalize_basic_info(raw_basic) -> dict:
    if not isinstance(raw_basic, dict):
        return {}

    basic_info = raw_basic.copy()
    for key in ("class_time", "target_class", "target_grade", "enrollment_notes"):
        basic_info[key] = normalize_no_data_value(basic_info.get(key))

    return basic_info


def course_term_exists(academic_year: str, academic_semester: str) -> bool:
    """Return whether the merged courses collection already has this term."""
    assert config.db_name, "DB_NAME must be set in .env file"

    collection_name = get_collection_name("courses")
    mydb = myclient[config.db_name]
    collection = mydb[collection_name]

    return (
        collection.count_documents(
            {
                "academic_year": int(academic_year),
                "academic_semester": int(academic_semester),
            },
            limit=1,
        )
        > 0
    )


def save_merged_courses_to_db(df: pd.DataFrame) -> None:
    """
    將合併後的完整課程資料 (Info + Detail) 寫入 MongoDB
    資料表名稱預設為: courses (或 courses_dev)
    """
    if df.empty:
        logger.error("Merged course DataFrame is empty")
        return

    assert config.db_name, "DB_NAME must be set in .env file"

    try:
        # 使用新的集合名稱，例如 'courses' 來存放完整的資料
        collection_name = get_collection_name("courses")
        logger.info(f"Saving merged courses to DB (collection: {collection_name})...")

        mydb = myclient[config.db_name]
        collection = mydb[collection_name]

        ensure_course_term_index(collection)

        # 1. 刪除不在目前資料中的舊資料 (Sync)
        term_filter = get_df_term_filter(df)
        current_codes = df["course_code"].tolist()
        delete_result = collection.delete_many(
            {**term_filter, "course_code": {"$nin": current_codes}}
        )
        logger.info(f"Deleted {delete_result.deleted_count} stale documents from {collection_name}")

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
            basic_info = normalize_basic_info(row.get("basic_info"))

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
            document["academic_year"] = int(row["academic_year"])
            document["academic_semester"] = int(row["academic_semester"])

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
                    get_course_term_filter(row), {"$set": document}, upsert=True
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

    assert config.db_name, "DB_NAME must be set in .env file"

    try:
        collection_name = get_collection_name("course_schedule")
        logger.info(f"Saving course schedule to DB (collection: {collection_name})...")
        mydb = myclient[config.db_name]
        collection = mydb[collection_name]

        collection.create_index("id")

        if df.empty:
            logger.warning("Course schedule DataFrame is empty, skipping save.")
            return

        # 1. 刪除不在目前資料中的舊資料 (Sync)
        current_ids = df["id"].tolist()
        delete_result = collection.delete_many({"id": {"$nin": current_ids}})
        logger.info(f"Deleted {delete_result.deleted_count} stale documents from {collection_name}")

        # 新增新資料
        records = df.to_dict(orient="records")
        ops = []
        for record in records:
            ops.append(
                UpdateOne({"id": record["id"]}, {"$set": record}, upsert=True)
            )

        if ops:
            result = collection.bulk_write(ops)
            logger.info(
                f"Write Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted: {result.upserted_count}"
            )

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

    assert config.db_name, "DB_NAME must be set in .env file"

    try:
        collection_name = get_collection_name("course_info")
        logger.info(f"Saving course info to DB (collection: {collection_name})...")
        mydb = myclient[config.db_name]
        collection = mydb[collection_name]
        ensure_course_term_index(collection)

        # 1. 刪除不在目前資料中的舊資料 (Sync)
        term_filter = get_df_term_filter(df)
        current_codes = df["course_code"].tolist()
        delete_result = collection.delete_many(
            {**term_filter, "course_code": {"$nin": current_codes}}
        )
        logger.info(f"Deleted {delete_result.deleted_count} stale documents from {collection_name}")

        ops = []
        records = df.to_dict(orient="records")

        for record in records:
            record["academic_year"] = int(record["academic_year"])
            record["academic_semester"] = int(record["academic_semester"])
            ops.append(
                UpdateOne(get_course_term_filter(record), {"$set": record}, upsert=True)
            )

        if ops:
            result = collection.bulk_write(ops)
            logger.info(
                f"Write Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted: {result.upserted_count}"
            )
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

    assert config.db_name, "DB_NAME must be set in .env file"

    try:
        collection_name = get_collection_name("course_detail")
        logger.info(f"Saving course detail to DB (collection: {collection_name})...")
        mydb = myclient[config.db_name]
        collection = mydb[collection_name]

        ensure_course_term_index(collection)

        # 1. 刪除不在目前資料中的舊資料 (Sync)
        term_filter = get_df_term_filter(df)
        current_codes = df["course_code"].tolist()
        delete_result = collection.delete_many(
            {**term_filter, "course_code": {"$nin": current_codes}}
        )
        logger.info(f"Deleted {delete_result.deleted_count} stale documents from {collection_name}")

        ops = []
        records = df.to_dict(orient="records")

        for row in records:
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
                "academic_year": int(row["academic_year"]),
                "academic_semester": int(row["academic_semester"]),
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
                "basic_info": normalize_basic_info(row.get("basic_info")),
            }

            ops.append(
                UpdateOne(
                    get_course_term_filter(row),
                    {"$set": document},
                    upsert=True,
                )
            )

        if ops:
            result = collection.bulk_write(ops)
            logger.info(
                f"Write Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted: {result.upserted_count}"
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

    assert config.db_name, "DB_NAME must be set in .env file"

    try:
        collection_name = get_collection_name("department_categories")
        logger.info(
            f"Saving department categories to DB (collection: {collection_name})..."
        )
        mydb = myclient[config.db_name]
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

        # 1. 刪除不在目前資料中的舊資料 (Sync)
        current_codes = df["category_code"].tolist()
        delete_result = collection.delete_many({"category_code": {"$nin": current_codes}})
        logger.info(f"Deleted {delete_result.deleted_count} stale documents from {collection_name}")

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

    assert config.db_name, "DB_NAME must be set in .env file"

    try:
        collection_name = get_collection_name("departments")
        logger.info(f"Saving departments to DB (collection: {collection_name})...")
        mydb = myclient[config.db_name]
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

        # 1. 刪除不在目前資料中的舊資料 (Sync)
        current_codes = df["department_code"].tolist()
        delete_result = collection.delete_many({"department_code": {"$nin": current_codes}})
        logger.info(f"Deleted {delete_result.deleted_count} stale documents from {collection_name}")

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
