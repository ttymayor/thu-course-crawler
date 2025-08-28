# THU Course Crawler

本專案用於自動化抓取東海大學選課相關資訊，並將資料存入 MongoDB。

## 功能

- 取得選課階段時間表
- 取得課程資訊和詳細資訊
- 資料清空後寫入 MongoDB

## 執行方式

使用 `uv` 套件：

1. 設定 `.env` 檔案，填入 MongoDB 連線資訊
2. 安裝依賴：
   ```bash
   uv sync
   ```
3. 執行爬蟲：
   ```bash
   uv run main.py
   ```
