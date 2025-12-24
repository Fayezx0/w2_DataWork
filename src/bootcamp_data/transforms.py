import re
from typing import Iterable

import pandas as pd

# Precompiled whitespace regex for normalize_text
_ws = re.compile(r"\s+")


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce key columns to string and numeric fields to nullable numeric dtypes."""

    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )


def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    """Return counts and proportions of missing values per column."""

    return (
        df.isna()
        .sum()
        .rename("n_missing")
        .to_frame()
        .assign(p_missing=lambda t: t["n_missing"] / len(df))
        .sort_values("p_missing", ascending=False)
    )


def add_missing_flags(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    """Add boolean missingness indicator columns for the provided columns."""

    out = df.copy()
    for c in cols:
        out[f"{c}__isna"] = out[c].isna()
    return out


def normalize_text(s: pd.Series) -> pd.Series:
    """Normalize text: strip, lowercase, collapse internal whitespace."""

    return (
        s.astype("string")
        .str.strip()
        .str.casefold()
        .str.replace(_ws, " ", regex=True)
    )


def apply_mapping(s: pd.Series, mapping: dict[str, str]) -> pd.Series:
    """Map known synonyms/typos while leaving unknown values unchanged."""

    return s.map(lambda x: mapping.get(x, x))


def dedupe_keep_latest(df: pd.DataFrame, key_cols: list[str], ts_col: str) -> pd.DataFrame:
    """Drop duplicates keeping the latest row by timestamp column."""

    return (
        df.sort_values(ts_col)
        .drop_duplicates(subset=key_cols, keep="last")
        .reset_index(drop=True)
    )


def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame:
    """Parse a column to datetime with coercion and optional UTC."""

    dt = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: dt})


def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    """Add common time part columns derived from a datetime column."""

    ts = df[ts_col]
    return df.assign(
        date=ts.dt.date,
        year=ts.dt.year,
        month=ts.dt.to_period("M").astype("string"),
        dow=ts.dt.day_name(),
        hour=ts.dt.hour,
    )


def iqr_bounds(s: pd.Series, k: float = 1.5) -> tuple[float, float]:
    """Return lower/upper bounds using IQR rule."""

    x = s.dropna()
    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k * iqr), float(q3 + k * iqr)


def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    """Cap values to given quantile range (keeps missing as-is)."""

    x = s.dropna()
    a, b = x.quantile(lo), x.quantile(hi)
    return s.clip(lower=a, upper=b)


def add_outlier_flag(df: pd.DataFrame, col: str, *, k: float = 1.5) -> pd.DataFrame:
    """Flag values outside IQR bounds."""

    lo, hi = iqr_bounds(df[col], k=k)
    return df.assign(**{f"{col}__is_outlier": (df[col] < lo) | (df[col] > hi)})

