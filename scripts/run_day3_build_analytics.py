import sys
from pathlib import Path
import logging
import pandas as pd

# --- 1. إصلاح المسارات (لضمان رؤية مجلد src) ---
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT / "src"))

from bootcamp_data.config import make_paths
from bootcamp_data.transforms import parse_datetime, add_time_parts, winsorize, add_outlier_flag
from bootcamp_data.joins import safe_left_join

# إعداد الـ Logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
log = logging.getLogger(__name__)

def main():
    paths = make_paths(ROOT)
    
    # 1. القراءة (Load)
    # نقرأ الملفات التي نظفناها في اليوم الأول
    orders_path = paths.processed / "orders.parquet" # تأكد أن هذا الملف موجود من تشغيل day1.py
    users_path = paths.processed / "users.parquet"   # تأكد أن هذا الملف موجود
    
    if not orders_path.exists() or not users_path.exists():
        log.error("❌ ملفات اليوم الأول غير موجودة! الرجاء تشغيل scripts/day1.py أولاً.")
        return

    log.info("Reading processed files...")
    orders = pd.read_parquet(orders_path)
    users = pd.read_parquet(users_path)

    # 2. الفحص (Verify) - بديل لملف quality.py
    # نتأكد أن جدول المستخدمين لا يحتوي تكرار (عشان ما ينفجر الدمج)
    assert users["user_id"].is_unique, "❌ كارثة: يوجد تكرار في user_id في جدول المستخدمين!"
    log.info("✅ Quality check passed: User IDs are unique.")

    # [cite_start]3. تحويل التواريخ (Time Transforms) [cite: 183]
    log.info("Parsing datetimes & adding time parts...")
    orders_t = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    # [cite_start]4. الدمج الآمن (Safe Join) [cite: 184]
    log.info("Joining orders with users...")
    joined = safe_left_join(
        orders_t,
        users,
        on="user_id",
        validate="many_to_one", # هذا السطر يحميك من التكرار
        suffixes=("", "_user")
    )

    # نتأكد أن عدد الصفوف لم يزد (دليل على نجاح الدمج)
    assert len(joined) == len(orders), f"❌ خطأ: عدد الصفوف تغير من {len(orders)} إلى {len(joined)}"
    log.info("✅ Join successful: Row counts matched.")

    # [cite_start]5. معالجة القيم الشاذة (Outliers) [cite: 184]
    log.info("Handling outliers...")
    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined = add_outlier_flag(joined, "amount", k=1.5)

    # 6. الحفظ النهائي (Save)
    out_path = paths.processed / "analytics_table.parquet"
    joined.to_parquet(out_path, index=False)
    
    log.info("------------------------------------------------")
    log.info(f"🎉 SUCCESS! Analytics table saved to:")
    log.info(f"   {out_path}")
    log.info("------------------------------------------------")
    
    # طباعة عينة سريعة
    print("\n--- Sample of the final table ---")
    print(joined[["order_id", "month", "country", "amount", "amount__is_outlier"]].head())

if __name__ == "__main__":
    main()