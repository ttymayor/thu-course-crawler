# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A crawler for Tunghai University (THU) course registration system (`course.thu.edu.tw`). It scrapes course info, course details, course schedules, and department data, then stores everything in MongoDB. Written in Python 3.13+, managed with `uv`.

## Commands

```bash
uv sync              # Install dependencies
uv run main.py       # Run full crawl pipeline (schedule -> courses)
uv run crawl_schedule.py    # Run only schedule crawler
uv run crawl_course.py      # Run only course crawler
uv run crawl_departments.py # Run only departments crawler
```

No tests or linter are configured.

## Environment Variables (.env)

Required: `DB_NAME`, `DB_URI`
Optional: `DB_ENV` (dev|prod, default: prod), `ACADEMIC_YEAR` (default: 114), `ACADEMIC_SEMESTER` (default: 2), `DEV_DATA_LIMIT` (default: 10)

When `DB_ENV=dev`, collection names get a `_dev` suffix and course detail fetching is limited to `DEV_DATA_LIMIT` items.

## Architecture

**Entry point**: `main.py` orchestrates crawlers by running `crawl_schedule.py` and `crawl_course.py` as subprocesses (via `subprocess.run`).

**Crawlers** (each runnable standalone):

- `crawl_schedule.py` — Scrapes the course selection schedule table from the THU homepage (sync, uses `requests`)
- `crawl_course.py` — Two-phase: (1) fetches course CSV via open data API, (2) scrapes individual course detail pages concurrently with `aiohttp` (semaphore-limited to 5). Merges info + detail DataFrames and saves the merged result to a single `courses` collection.
- `crawl_departments.py` — Scrapes department categories and individual departments from the dept listing pages (sync)

**Data flow in `crawl_course.py`**: CSV download -> DataFrame -> detail HTML scraping (concurrent) -> DataFrame -> left join on `course_code` -> MongoDB upsert

**`db.py`**: All MongoDB write operations. Each save function syncs data by deleting stale documents (not in current dataset) then upserting via `bulk_write`. Collections: `courses`, `course_schedule`, `course_info`, `course_detail`, `department_categories`, `departments`.

**`config.py`**: Loads `.env` via `python-dotenv` into a `Config` dataclass. A global `config` singleton is created at import time.

**`utils/`**: DataFrame column renaming/processing (`dataframe_utils.py`), datetime parsing for schedule timestamps (`datetime_to_timestamp.py`), course info fetch helpers (`course_utils.py`), centralized logging with Rich (`logger.py`).

## Key Conventions

- All datetimes use Asia/Taipei timezone and ISO 8601 format
- Chinese column names from source data are renamed to English in `process_*_df` functions
- Course detail parsing uses CSS selectors tied to specific THU page structure (`#mainContent > div:nth-child(...)`)
- Logging uses `rich.logging.RichHandler` via `setup_logger()` / `get_logger()` pattern
