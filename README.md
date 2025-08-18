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

## GitHub Actions Workflows

本專案包含以下自動化流程：

### CI/CD Pipeline (`ci.yml`)

- **觸發條件**：推送到 main/develop 分支、Pull Request、每日定時執行
- **功能**：
  - 代碼品質檢查（ruff linting）
  - 類型檢查（mypy）
  - 自動化測試
  - 安全掃描（bandit）
  - 定時資料爬取

### Release and Deploy (`deploy.yml`)

- **觸發條件**：發布 Release、手動觸發
- **功能**：
  - 建構部署套件
  - 上傳部署檔案
  - 通知部署結果

### 環境設定

在 GitHub repository 設定以下 Secrets：

- `MONGODB_URI`：MongoDB 連線字串
- `DATABASE_NAME`：資料庫名稱（選用，預設為 `thu_course`）

### Docker 支援

如需容器化部署：

```bash
# 建構映像
docker build -t thu-course-crawler .

# 執行容器
docker run -e MONGODB_URI="your_mongodb_uri" thu-course-crawler
```

## 聯絡

如有問題請聯絡專案負責人。
