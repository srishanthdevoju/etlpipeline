# ===========================
# validate.py
# ===========================

import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

TABLE_NAME = "telco_churn_cleaned"
TRANSFORMED_FILE = "churn_transformed.csv"


# ---------------------------------------------------------
# Supabase Client
# ---------------------------------------------------------
def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")

    return create_client(url, key)


# ---------------------------------------------------------
# Validation Script
# ---------------------------------------------------------
def validate_data():
    supabase = get_supabase_client()

    # Build staged path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_path = os.path.join(base_dir, "data", "staged", TRANSFORMED_FILE)

    if not os.path.exists(staged_path):
        raise FileNotFoundError(f"❌ Transformed CSV not found: {staged_path}")

    # Load transformed CSV
    df = pd.read_csv(staged_path)
    row_count_csv = len(df)

    print("\n==============================")
    print("🔍 VALIDATION REPORT")
    print("==============================")

    # ------------------------------------
    # 1. Missing Value Checks
    # ------------------------------------
    numeric_required = ["tenure", "monthlycharges", "totalcharges"]

    missing_report = df[numeric_required].isnull().sum()

    print("\n📌 Missing Value Check:")
    print(missing_report)

    if missing_report.sum() == 0:
        print("✅ No missing values in required numeric columns.")
    else:
        print("❌ Missing values detected! Fix before loading.")
        return

    # ------------------------------------
    # 2. Unique Row Count check
    # ------------------------------------
    unique_rows = len(df.drop_duplicates())
    print(f"\n📌 Unique rows in transformed CSV: {unique_rows}")

    if unique_rows == row_count_csv:
        print("✅ No duplicate rows found.")
    else:
        print("⚠ Warning: Duplicate rows detected.")

    # ------------------------------------
    # 3. Supabase row count check
    # ------------------------------------
    supabase_count = supabase.table(TABLE_NAME).select("*").execute()
    row_count_supabase = len(supabase_count.data)

    print(f"\n📌 Row count in CSV       : {row_count_csv}")
    print(f"📌 Row count in Supabase  : {row_count_supabase}")

    if row_count_csv == row_count_supabase:
        print("✅ CSV and Supabase row counts match.")
    else:
        print("❌ Mismatch! Load incomplete or incorrect.")

    # ------------------------------------
    # 4. Segment Category Checks
    # ------------------------------------
    print("\n📌 Segment Category Checks:")

    expected_tenure_groups = {"New", "Regular", "Loyal", "Champion"}
    expected_charge_segments = {"Low", "Medium", "High"}

    actual_tenure_groups = set(df["tenure_group"].unique())
    actual_charge_segments = set(df["monthly_charge_segment"].unique())

    print(f"   tenure_group values: {actual_tenure_groups}")
    print(f"   Expected: {expected_tenure_groups}")

    print(f"   monthly_charge_segment values: {actual_charge_segments}")
    print(f"   Expected: {expected_charge_segments}")

    if actual_tenure_groups == expected_tenure_groups:
        print("✅ tenure_group contains all categories.")
    else:
        print("❌ tenure_group missing categories!")

    if actual_charge_segments == expected_charge_segments:
        print("✅ monthly_charge_segment contains all categories.")
    else:
        print("❌ monthly_charge_segment missing categories!")

    # ------------------------------------
    # 5. Contract code check
    # ------------------------------------
    print("\n📌 Contract Code Check:")

    valid_codes = {0, 1, 2}
    contract_codes = set(df["contract_type_code"].unique())

    print(f"   contract_type_code values: {contract_codes}")

    if contract_codes.issubset(valid_codes):
        print("✅ Contract type codes are valid.")
    else:
        print("❌ Invalid contract type codes detected!")

    # ------------------------------------
    # Final Summary
    # ------------------------------------
    print("\n==============================")
    print("🎯 VALIDATION COMPLETED")
    print("==============================\n")


if __name__ == "__main__":
    validate_data()
