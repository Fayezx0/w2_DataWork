import pandas as pd

# --- 1. دوال التنظيف الأساسية (من اليوم الأول) ---
def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    تنظيف الأنواع: تحويل النصوص إلى أرقام وتواريخ صحيحة.
    """
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        # تحويل الأرقام مع تحويل الأخطاء إلى NaN
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )

# --- 2. دوال الوقت (الجديدة لليوم الثالث) ⏰ ---
def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame:
    """تحويل عمود نصي إلى تاريخ حقيقي (Datetime)"""
    dt = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: dt})

def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    """استخراج (الشهر، السنة، اليوم) من عمود التاريخ لتسهيل التحليل"""
    ts = df[ts_col]
    return df.assign(
        date=ts.dt.date,
        year=ts.dt.year,
        month=ts.dt.to_period("M").astype("string"), # مثال: '2025-01'
        dow=ts.dt.day_name(),   # مثال: 'Monday'
        hour=ts.dt.hour,
    )

# --- 3. دوال القيم الشاذة (الجديدة لليوم الثالث) 📈 ---
def iqr_bounds(s: pd.Series, k: float = 1.5) -> tuple[float, float]:
    """حساب الحدود الطبيعية للبيانات (ما فوقها أو تحتها يعتبر شاذاً)"""
    x = s.dropna()
    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k * iqr), float(q3 + k * iqr)

def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    """تقليم القيم المتطرفة جداً (Capping) بدلاً من حذفها"""
    x = s.dropna()
    a, b = x.quantile(lo), x.quantile(hi)
    return s.clip(lower=a, upper=b)

def add_outlier_flag(df: pd.DataFrame, col: str, *, k: float = 1.5) -> pd.DataFrame:
    """إضافة عمود جديد (True/False) يحدد هل القيمة شاذة أم لا"""
    lo, hi = iqr_bounds(df[col], k=k)
    return df.assign(**{f"{col}__is_outlier": (df[col] < lo) | (df[col] > hi)})