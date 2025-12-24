from __future__ import annotations
import pandas as pd

def safe_left_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: str | list[str],
    *,
    validate: str,
    suffixes: tuple[str, str] = ("", "_r"),
) -> pd.DataFrame:
    """
    A safer wrapper around pd.merge for left joins.
    It forces you to specify the validation rule (e.g., "many_to_one")
    to prevent accidental row multiplication.
    """
    return left.merge(
        right, 
        how="left", 
        on=on, 
        validate=validate, 
        suffixes=suffixes
    )