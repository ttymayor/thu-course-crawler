# THU Course Crawler

本專案用於自動化抓取東海大學選課相關資訊，並將資料存入 MongoDB。

## 功能

- 取得選課階段時間表，並轉換為標準格式
- 取得課程資訊（含課程代碼、名稱、學分等），自動保留前導零
- 資料自動寫入 MongoDB，避免重複

## 執行方式

1. 安裝依賴：
   ```bash
   uv pip install -r requirements.txt
   ```
2. 設定 `.env` 檔案，填入 MongoDB 連線資訊
3. 執行主程式：
   ```bash
   uv run main.py
   ```

## 注意事項

- 課程代碼（course_code）與系所代碼（department_code）皆強制為 string 型別，確保前導零不遺失
- 資料庫寫入前會自動清空 collection，僅保留最新資料

## 聯絡

如有問題請聯絡專案負責人。
