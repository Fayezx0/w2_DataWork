from pathlib import Path
import logging
import sys

# Make `src/` importable when running as a script
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from bootcamp_data.config import make_paths
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_unique_key,
)
from bootcamp_data.transforms import (
    parse_datetime,
    add_time_parts,
    winsorize,
    add_outlier_flag,
)
from bootcamp_data.joins import safe_left_join

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    p = make_paths(ROOT)

    orders = pd.read_parquet(p.processed / "orders_clean.parquet")
    users = pd.read_parquet(p.processed / "users.parquet")

    # Basic checks
    require_columns(orders, ["order_id", "user_id", "amount", "quantity", "created_at", "status_clean"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders, "orders_clean")
    assert_non_empty(users, "users")
    assert_unique_key(users, "user_id")

    # Time parsing + parts
    orders_t = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )
    n_missing_ts = int(orders_t["created_at"].isna().sum())
    log.info("missing created_at after parse: %s / %s", n_missing_ts, len(orders_t))

    # Safe join (orders many -> users one)
    joined = safe_left_join(
        orders_t,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )
    assert len(joined) == len(orders_t), "Row count changed (join explosion?)"
    match_rate = 1.0 - float(joined["country"].isna().mean())
    log.info("rows after join: %s", len(joined))
    log.info("country match rate: %.3f", match_rate)

    # Outlier handling
    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined = add_outlier_flag(joined, "amount", k=1.5)

    # Optional: small summary table
    revenue_by_country = (
        joined.groupby("country", dropna=False)
        .agg(
            revenue=("amount", "sum"),
            orders=("order_id", "count"),
        )
        .reset_index()
    )
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    revenue_path = reports_dir / "revenue_by_country.csv"
    revenue_by_country.to_csv(revenue_path, index=False)
    log.info("wrote revenue summary: %s", revenue_path)

    # Write final analytics table
    out_path = p.processed / "analytics_table.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    joined.to_parquet(out_path, index=False)
    log.info("wrote analytics table: %s", out_path)


if __name__ == "__main__":
    main()


