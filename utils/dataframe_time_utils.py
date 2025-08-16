import pandas as pd
from utils.datetime_to_timestamp import range_str_to_timestamps, str_to_isotime

def process_course_schedule_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    拆分 '起迄時間' 欄位為 start_time, end_time 並轉 timestamp，'結果公布日' 轉 iso 格式（非標準則保留原值）。
    """
    def split_range(row):
        try:
            start, end = range_str_to_timestamps(row['起迄時間'])
            return pd.Series({'start_time': start, 'end_time': end})
        except Exception:
            return pd.Series({'start_time': None, 'end_time': None})

    times = df.apply(split_range, axis=1)
    df = pd.concat([df, times], axis=1)

    def safe_str_to_isotime(val):
        try:
            return str_to_isotime(val)
        except Exception:
            return val
    df['result_publish_time'] = df['結果公布日'].apply(safe_str_to_isotime)

    # 篩選掉不需要的欄位
    df = df[['選課階段', '狀態', 'start_time', 'end_time', 'result_publish_time']]
    df = df.rename(columns={
        '選課階段': 'course_stage',
        '狀態': 'status',
    })
    return df

def process_course_info_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    對課程資訊 DataFrame 進行處理
    """
    df = df.rename(columns={
        '學年': 'academic_year',
        '學期': 'academic_semester',
        '選課代碼': 'course_code',
        '課程名稱': 'course_name',
        '開課系所代碼': 'department_code',
        '開課系所名稱': 'department_name',
        '必選修': 'course_type',
        '學分1': 'credits_1',
        '學分2': 'credits_2',
    })
    return df
