from datetime import datetime
from zoneinfo import ZoneInfo

def str_to_isotime(dt_str: str) -> str:
    """Convert a datetime string 'YYYY-MM-DD HH:MM:SS' to ISO 8601 format."""
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("Asia/Taipei")).isoformat()
    return dt

def range_str_to_timestamps(range_str: str) -> tuple[str, str]:
    """Convert a range string 'YYYY-MM-DD HH:MM:SS ~ YYYY-MM-DD HH:MM:SS' to two ISO 8601 format strings."""
    start_str, end_str = [s.strip() for s in range_str.split('~')]
    return str_to_isotime(start_str), str_to_isotime(end_str)
