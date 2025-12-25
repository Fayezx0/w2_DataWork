from __future__ import annotations
from datetime import datetime, timezone
import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.joins import safe_left_join
from bootcamp_data.quality import assert_non_empty, assert_unique_key, require_columns
from bootcamp_data.transforms import (
    add_missing_flags,
    add_outlier_flag,
    add_time_parts,
    apply_mapping,
    enforce_schema,
    normalize_text,
    parse_datetime,
    winsorize,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path


def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read raw CSVs using io helpers."""
    log.info("Loading inputs from %s and %s", cfg.raw_orders, cfg.raw_users)
    orders = read_orders_csv(cfg.raw_orders)
    users = read_users_csv(cfg.raw_users)
    return orders, users


def transform(orders_raw: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    """Transform and enrich data."""
    # Fail-fast checks
    require_columns(
        orders_raw,
        ["order_id", "user_id", "amount", "quantity", "created_at", "status"],
    )
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")

    # The "one" side must be unique for a many->one join
    assert_unique_key(users, "user_id")

    status_map = {"paid": "paid", "refund": "refund", "refunded": "refund"}

    log.info("Cleaning orders")
    orders = (
        orders_raw.pipe(enforce_schema)
        .assign(
            status_clean=lambda d: apply_mapping(normalize_text(d["status"]), status_map)
        )
        .pipe(add_missing_flags, cols=["amount", "quantity"])
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    log.info("Joining orders with users")
    joined = safe_left_join(
        orders,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )

    # Left join should not change row count
    assert len(joined) == len(orders), "Row count changed (join explosion?)"

    log.info("Adding outlier flags and winsorized amount")
    # Outliers: keep raw `amount`, add winsorized + outlier flag for analysis
    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined = add_outlier_flag(joined, "amount", k=1.5)

    return joined


def load_outputs(
    *, analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig
) -> None:
    """Write processed artifacts (idempotent)."""
    log.info("Writing processed users to %s", cfg.out_users)
    write_parquet(users, cfg.out_users)

    log.info("Writing analytics table to %s", cfg.out_analytics)
    write_parquet(analytics, cfg.out_analytics)

    # Write an orders-only table by dropping user-side columns
    user_side_cols = [c for c in users.columns if c != "user_id"]
    cols_to_drop = [c for c in user_side_cols if c in analytics.columns] + [
        c for c in analytics.columns if c.endswith("_user")
    ]
    orders_clean = analytics.drop(columns=cols_to_drop, errors="ignore")

    log.info("Writing clean orders to %s", cfg.out_orders_clean)
    write_parquet(orders_clean, cfg.out_orders_clean)


def write_run_meta(
    cfg: ETLConfig,
    *,
    orders_raw: pd.DataFrame,
    users: pd.DataFrame,
    analytics: pd.DataFrame,
) -> None:
    """Write run metadata JSON."""
    missing_created_at = (
        int(analytics["created_at"].isna().sum())
        if "created_at" in analytics.columns
        else None
    )
    country_match_rate = (
        1.0 - float(analytics["country"].isna().mean())
        if "country" in analytics.columns
        else None
    )

    meta = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "rows_in_orders_raw": int(len(orders_raw)),
        "rows_in_users": int(len(users)),
        "rows_out_analytics": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
        "config": {k: str(v) for k, v in asdict(cfg).items()},
    }

    log.info("Writing run metadata to %s", cfg.run_meta)
    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def run_etl(cfg: ETLConfig) -> None:
    """Orchestrate the ETL pipeline."""
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    log.info("Starting ETL run")
    orders_raw, users = load_inputs(cfg)

    log.info("Transforming (orders=%s, users=%s)", len(orders_raw), len(users))
    analytics = transform(orders_raw, users)

    log.info("Writing outputs")
    load_outputs(analytics=analytics, users=users, cfg=cfg)

    log.info("Writing metadata")
    write_run_meta(cfg, orders_raw=orders_raw, users=users, analytics=analytics)
    log.info("ETL run complete")
