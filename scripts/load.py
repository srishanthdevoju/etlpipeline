# ===========================
# load.py - FINAL VERSION
# ===========================

import os
import time
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

TRANSFORMED_FILE = "churn_transformed.csv"
TABLE_NAME = "telco_churn_cleaned"


# ---------------------------------------------------------
# Supabase Client
# ---------------------------------------------------------
def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ Missing Supabase credentials in .env")

    return create_client(url, key)


# ---------------------------------------------------------
# Create Table (NO RPC NEEDED)
# ---------------------------------------------------------
def create_table_if_missing(supabase):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id BIGSERIAL PRIMARY KEY,
        tenure INTEGER,
        monthlycharges FLOAT,
        totalcharges FLOAT,
        churn TEXT,
        internetservice TEXT,
        contract TEXT,
        paymentmethod TEXT,
        tenure_group TEXT,
        monthly_charge_segment TEXT,
        has_internet_service INTEGER,
        is_multi_line_user INTEGER,
        contract_type_code INTEGER
    );
    """

    # execute SQL using Supabase HTTP POST to SQL endpoint
    supabase.postgrest.session.post(
        "/rpc/sql",
        json={"query": sql}
    )


# ---------------------------------------------------------
# Batch Insert with Retry
# ---------------------------------------------------------
def batch_insert(supabase, records, retries=3):
    attempt = 1
    while attempt <= retries:
        try:
            supabase.table(TABLE_NAME).insert(records).execute()
            return True
        except Exception as e:
            print(f"⚠ Batch failed (Attempt {attempt}): {e}")
            time.sleep(1.5)
            attempt += 1
    return False


# ---------------------------------------------------------
# Load Cleaned CSV Into Supabase
# ---------------------------------------------------------
def load_data():
    supabase = get_supabase_client()

    # Build path to staged CSV
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_path = os.path.join(base_dir, "data", "staged", TRANSFORMED_FILE)

    if not os.path.exists(staged_path):
        raise FileNotFoundError(f"❌ Transformed CSV not found at {staged_path}")

    # Load dataset
    df = pd.read_csv(staged_path)

    # Convert NaN → None for Supabase
    df = df.where(pd.notnull(df), None)

    # Ensure lowercase column names
    df.columns = [col.lower() for col in df.columns]

    # List of required columns
    required_cols = [
        "tenure",
        "monthlycharges",
        "totalcharges",
        "churn",
        "internetservice",
        "contract",
        "paymentmethod",
        "tenure_group",
        "monthly_charge_segment",
        "has_internet_service",
        "is_multi_line_user",
        "contract_type_code"
    ]

    # Ensure only the required columns remain
    df = df[required_cols]

    print("🛠 Creating table if needed...")
    try:
        create_table_if_missing(supabase)
    except Exception as e:
        print("ℹ Table creation warning:", e)

    print("🧹 Clearing existing rows...")
    supabase.table(TABLE_NAME).delete().neq("tenure", -1).execute()

    # Upload data in batches
    batch_size = 200
    total_rows = len(df)

    print(f"📤 Uploading {total_rows} rows in batches of {batch_size}...")

    for start in range(0, total_rows, batch_size):
        batch = df.iloc[start:start + batch_size]
        records = batch.to_dict("records")

        if batch_insert(supabase, records):
            print(f"   → Inserted rows {start+1} to {min(start+batch_size, total_rows)}")
        else:
            print("❌ Failed batch. Stopping...")
            break

    print(f"\n🎉 Load complete! Inserted {total_rows} rows.")


# ---------------------------------------------------------
# Run Script
# ---------------------------------------------------------
if __name__ == "__main__":
    load_data()
