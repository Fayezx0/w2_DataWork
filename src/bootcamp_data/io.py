"""Input/output utilities for data processing."""

import pandas as pd
from pathlib import Path
from typing import Union

from .config import RAW_DATA_DIR, PROCESSED_DATA_DIR, CACHE_DIR


def load_raw_data(filename: str, **kwargs) -> pd.DataFrame:
    """Load raw data from the raw data directory.
    
    Args:
        filename: Name of the file to load
        **kwargs: Additional arguments passed to pandas read function
        
    Returns:
        DataFrame containing the loaded data
    """
    filepath = RAW_DATA_DIR / filename
    if not filepath.exists():
        raise FileNotFoundError(f"Raw data file not found: {filepath}")
    
    # Infer file type from extension
    ext = filepath.suffix.lower()
    if ext == '.csv':
        return pd.read_csv(filepath, **kwargs)
    elif ext in ['.xlsx', '.xls']:
        return pd.read_excel(filepath, **kwargs)
    elif ext == '.json':
        return pd.read_json(filepath, **kwargs)
    elif ext == '.parquet':
        return pd.read_parquet(filepath, **kwargs)
    else:
        raise ValueError(f"Unsupported file type: {ext}")


def save_processed_data(df: pd.DataFrame, filename: str, **kwargs) -> None:
    """Save processed data to the processed data directory.
    
    Args:
        df: DataFrame to save
        filename: Name of the file to save
        **kwargs: Additional arguments passed to pandas write function
    """
    filepath = PROCESSED_DATA_DIR / filename
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    # Infer file type from extension
    ext = filepath.suffix.lower()
    if ext == '.csv':
        df.to_csv(filepath, index=False, **kwargs)
    elif ext in ['.xlsx', '.xls']:
        df.to_excel(filepath, index=False, **kwargs)
    elif ext == '.json':
        df.to_json(filepath, **kwargs)
    elif ext == '.parquet':
        df.to_parquet(filepath, **kwargs)
    else:
        raise ValueError(f"Unsupported file type: {ext}")


def load_cache(filename: str, **kwargs) -> pd.DataFrame:
    """Load cached data from the cache directory.
    
    Args:
        filename: Name of the cached file to load
        **kwargs: Additional arguments passed to pandas read function
        
    Returns:
        DataFrame containing the cached data
    """
    filepath = CACHE_DIR / filename
    if not filepath.exists():
        raise FileNotFoundError(f"Cache file not found: {filepath}")
    
    ext = filepath.suffix.lower()
    if ext == '.csv':
        return pd.read_csv(filepath, **kwargs)
    elif ext == '.parquet':
        return pd.read_parquet(filepath, **kwargs)
    elif ext == '.pkl':
        return pd.read_pickle(filepath, **kwargs)
    else:
        raise ValueError(f"Unsupported cache file type: {ext}")


def save_cache(df: pd.DataFrame, filename: str, **kwargs) -> None:
    """Save data to the cache directory.
    
    Args:
        df: DataFrame to cache
        filename: Name of the cache file
        **kwargs: Additional arguments passed to pandas write function
    """
    filepath = CACHE_DIR / filename
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    ext = filepath.suffix.lower()
    if ext == '.csv':
        df.to_csv(filepath, index=False, **kwargs)
    elif ext == '.parquet':
        df.to_parquet(filepath, **kwargs)
    elif ext == '.pkl':
        df.to_pickle(filepath, **kwargs)
    else:
        raise ValueError(f"Unsupported cache file type: {ext}")

