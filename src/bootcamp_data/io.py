from pathlib import Path
import pandas as pd

# Common NA markers to keep missing values consistent across loaders.
NA = ["", "NA", "N/A", "null", "None"]


def read_orders_csv(path: Path) -> pd.DataFrame:
    """Read orders CSV with explicit dtypes and NA handling."""

    return pd.read_csv(
        path,
        dtype={"order_id": "string", "user_id": "string"},
        na_values=NA,
        keep_default_na=True,
    )


def read_users_csv(path: Path) -> pd.DataFrame:
    """Read users CSV with explicit dtypes and NA handling."""

    return pd.read_csv(
        path,
        dtype={"user_id": "string"},
        na_values=NA,
        keep_default_na=True,
    )


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to Parquet, ensuring parent directories exist."""

    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def read_parquet(path: Path) -> pd.DataFrame:
    """Read a Parquet file into a DataFrame."""

    return pd.read_parquet(path)

