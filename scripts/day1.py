# from bootcamp_data.transforms import enforce_schema
# from bootcamp_data.io import read_orders_csv
# from pathlib import Path

# ROOT = Path(__file__)
# df = read_orders_csv(ROOT / "data" / "orders.csv")
# df_enforced = enforce_schema(df)


# # 1. عرض النتيجة للتأكد (Inspection)
# print("--- Old Types (Dirty) ---")
# print(df.dtypes)
# print("\n--- New Types (Clean) ---")
# print(df_enforced.dtypes) # ستلاحظ ظهور Int64 و string

# # 2. عرض أول 5 صفوف نظيفة
# print("\n--- First 5 Clean Rows ---")
# print(df_enforced.head())

# # 3. حفظ الملف النظيف (Saving)
# # نحفظه بصيغة Parquet السريعة لاستخدامه لاحقاً في التحليلع
# output_path = ROOT / "data" / "orders_clean.parquet"
# df_enforced.to_parquet(output_path, index=False)

# print(f"\n✅ Success! Cleaned data saved to: {output_path}")


import sys
from pathlib import Path
import logging

# 1. إعداد المسارات لكي يرى بايثون مجلد src
# هذا السطر ضروري جداً لتجنب خطأ ModuleNotFoundError
ROOT = Path(__file__).resolve().parents[1]  # يرجع خطوتين للوراء (إلى المجلد الرئيسي)
sys.path.append(str(ROOT / "src"))

# الآن يمكننا استدعاء ملفاتنا
from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import enforce_schema

# إعداد الـ Logging لرؤية النتائج في التيرمنال
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def main():
    # 2. تجهيز المسارات
    paths = make_paths(ROOT)
    
    log.info("Starting ETL process...")
    
    # 3. قراءة البيانات الخام (Raw)
    # لاحظ أننا نقرأ من paths.raw
    log.info(f"Reading orders from {paths.raw}")
    orders_df = read_orders_csv(paths.raw / "orders.csv")
    
    # 4. تنظيف البيانات (Transform)
    log.info("Cleaning orders data...")
    orders_clean = enforce_schema(orders_df)
    
    # 5. حفظ البيانات المعالجة (Processed)
    output_path = paths.processed / "orders.parquet"
    log.info(f"Writing clean data to {output_path}")
    write_parquet(orders_clean, output_path)
    
    log.info("✅ Done! Processed file created successfully.")

if __name__ == "__main__":
    main()