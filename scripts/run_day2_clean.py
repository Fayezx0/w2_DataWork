import logging
import sys
from pathlib import Path

# Setup paths to import from src/
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import (
    enforce_schema,
    missingness_report,
    add_missing_flags,
    normalize_text,
    apply_mapping,
)
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_in_range,
)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

def main() -> None:
    p = make_paths(ROOT)

    # 1. Load Raw Data
    log.info("Loading raw inputs...")
    orders_raw = read_orders_csv(p.raw / "orders.csv")
    users = read_users_csv(p.raw / "users.csv")
    
    # 2. Quality Checks (Fail Fast)
    log.info("Running quality checks...")
    require_columns(orders_raw, ["order_id", "user_id", "amount", "status"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")

    # 3. Enforce Schema & Report Missingness
    orders = enforce_schema(orders_raw)
    
    # Save missingness report before cleaning
    rep = missingness_report(orders)
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    rep.to_csv(reports_dir / "missingness_orders.csv")
    log.info("Saved missingness report to reports/")

    # 4. Clean & Transform
    # Normalize status (e.g., 'Paid ' -> 'paid')
    status_norm = normalize_text(orders["status"])
    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    status_clean = apply_mapping(status_norm, mapping)

    orders_clean = (
        orders.assign(status_clean=status_clean)
              .pipe(add_missing_flags, cols=["amount", "quantity"])
    )

    # 5. Logic Checks (Range)
    assert_in_range(orders_clean["amount"], lo=0, name="amount")

    # 6. Save Processed Output
    write_parquet(orders_clean, p.processed / "orders_clean.parquet")
    write_parquet(users, p.processed / "users.parquet")
    log.info(f"Success! Processed files saved to {p.processed}")

if __name__ == "__main__":
    main()