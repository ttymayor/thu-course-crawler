from datetime import datetime
from zoneinfo import ZoneInfo

TAIPEI_TZ = ZoneInfo("Asia/Taipei")


def str_to_isotime(dt_str: str) -> str:
    """Convert THU datetime strings to ISO 8601 format."""
    normalized = str(dt_str).strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(normalized, fmt).replace(tzinfo=TAIPEI_TZ).isoformat()
        except ValueError:
            continue
    raise ValueError(f"Unsupported datetime format: {dt_str}")


def range_str_to_timestamps(range_str: str) -> tuple[str, str]:
    """Convert a THU datetime range string to two ISO 8601 format strings."""
    start_str, end_str = [s.strip() for s in range_str.split("~")]
    return str_to_isotime(start_str), str_to_isotime(end_str)
