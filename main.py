import subprocess
import sys
from datetime import datetime


def main() -> None:
    """主函數 - 執行所有爬蟲任務"""
    print(f"開始執行完整爬蟲任務 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 30)

    try:
        # 1. 執行選課時間表爬蟲
        print("1. 執行選課時間表爬蟲...")
        result = subprocess.run(
            [sys.executable, "crawl_schedule.py"], capture_output=True, text=True
        )
        if result.returncode == 0:
            print("   [OK] 選課時間表爬蟲完成")
            print(result.stdout)
        else:
            print(f"   [ERROR] 選課時間表爬蟲失敗: {result.stderr}")
            return

        # 2. 執行課程爬蟲（資訊+詳細資訊）
        print("2. 執行課程爬蟲...")
        result = subprocess.run(
            [sys.executable, "crawl_course_info.py"], capture_output=True, text=True
        )
        if result.returncode == 0:
            print("   [OK] 課程爬蟲完成")
            print(result.stdout)
        else:
            print(f"   [ERROR] 課程爬蟲失敗: {result.stderr}")
            return

        print("=" * 30)
        print(f"所有爬蟲任務完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 30)

    except Exception as e:
        print(f"爬蟲任務執行失敗: {e}")
        raise


if __name__ == "__main__":
    main()
