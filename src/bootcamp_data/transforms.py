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
