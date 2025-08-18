# 使用 Python 3.13 slim 基礎映像
FROM python:3.13-slim

# 設定工作目錄
WORKDIR /app

# 安裝系統依賴
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 安裝 uv
RUN pip install uv

# 複製專案檔案
COPY . .

# 安裝 Python 依賴
RUN uv sync --frozen

# 建立非 root 使用者
RUN useradd -m -u 1000 crawler && chown -R crawler:crawler /app
USER crawler

# 設定環境變數
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# 預設執行命令
CMD ["uv", "run", "main.py"]
