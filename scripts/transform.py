import os
import pandas as pd
import numpy as np


def transform_data(raw_filename="WA_Fn-UseC_-Telco-Customer-Churn.csv",
                   output_filename="churn_transformed.csv"):
    """
    Reads raw CSV → cleans → feature engineers → selects required columns →
    saves result to data/staged/
    """

    # -------------------------
    # Construct file paths
    # -------------------------
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    raw_path = os.path.join(base_dir, "data", "raw", raw_filename)
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)
    staged_path = os.path.join(staged_dir, output_filename)

    # -------------------------
    # Load raw data
    # -------------------------
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"❌ Raw file not found at {raw_path}")

    df = pd.read_csv(raw_path)

    # -------------------------
    # Cleaning
    # -------------------------
    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")

    numeric_cols = ["tenure", "MonthlyCharges", "TotalCharges"]
    for col in numeric_cols:
        df[col] = df[col].fillna(df[col].median())

    # Fill missing categorical values
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].fillna("Unknown")

    # -------------------------
    # Feature Engineering
    # -------------------------

    # 1. tenure_group
    df["tenure_group"] = df["tenure"].apply(
        lambda x: "New" if x <= 12 else
                  "Regular" if x <= 36 else
                  "Loyal" if x <= 60 else
                  "Champion"
    )

    # 2. monthly_charge_segment
    df["monthly_charge_segment"] = df["MonthlyCharges"].apply(
        lambda x: "Low" if x < 30 else
                  "Medium" if x <= 70 else "High"
    )

    # 3. has_internet_service
    df["has_internet_service"] = df["InternetService"].map({
        "DSL": 1,
        "Fiber optic": 1,
        "No": 0
    })

    # 4. is_multi_line_user
    df["is_multi_line_user"] = df["MultipleLines"].apply(
        lambda x: 1 if x == "Yes" else 0
    )

    # 5. contract_type_code
    df["contract_type_code"] = df["Contract"].map({
        "Month-to-month": 0,
        "One year": 1,
        "Two year": 2
    })

    # -------------------------
    # Prepare for Supabase
    # Keep ONLY required columns
    # -------------------------

    required_cols = [
        "tenure",
        "MonthlyCharges",
        "TotalCharges",
        "Churn",
        "InternetService",
        "Contract",
        "PaymentMethod",
        "tenure_group",
        "monthly_charge_segment",
        "has_internet_service",
        "is_multi_line_user",
        "contract_type_code"
    ]

    df = df[required_cols]

    # Convert ALL column names to lowercase (CRITICAL)
    df.columns = [col.lower() for col in df.columns]

    # -------------------------
    # Save output
    # -------------------------
    df.to_csv(staged_path, index=False)
    print(f"✅ Transform complete → {staged_path}")

    return staged_path


if __name__ == "__main__":
    transform_data()
