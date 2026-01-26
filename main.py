import sys
import subprocess
from utils.logger import setup_logger, get_logger

setup_logger()
logger = get_logger(__name__)


def main() -> None:
    """主函數 - 執行所有爬蟲任務"""
    logger.info("[main] Starting crawling tasks")

    try:
        # --- 1. 執行選課時間表爬蟲 ---
        logger.info("[main] 1. Executing crawl_schedule.py...")
        result_schedule = subprocess.run(
            [sys.executable, "crawl_schedule.py"],
            capture_output=False,
            text=True,
        )

        if result_schedule.returncode == 0:
            logger.info("[main] 1. Done")
        else:
            logger.error(f"Schedule crawling failed: {result_schedule.stderr}")
            return

        # --- 2. 執行課程爬蟲（資訊+詳細資訊） ---
        logger.info("[main] 2. Executing crawl_course.py...")
        result_course = subprocess.run(
            [sys.executable, "crawl_course.py"],
            capture_output=False,
            text=True,
        )

        if result_course.returncode == 0:
            logger.info("[main] 2. Done")
        else:
            logger.error(f"Course crawling failed: {result_course.stderr}")
            return

        logger.info("[main] All crawling tasks completed.")

    except Exception as e:
        logger.error(f"爬蟲任務執行失敗: {e}")
        raise


if __name__ == "__main__":
    main()
