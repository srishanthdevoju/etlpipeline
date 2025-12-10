import os
import pandas as pd

def load_local_data(filename="WA_Fn-UseC_-Telco-Customer-Churn.csv"):
    """
    Reads a CSV file from the data/raw directory.
    """
    # 1. Construct the path relative to this script
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = os.path.join(base_dir, "data", "raw", filename)

    # 2. Check if file exists before trying to load
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        print("   (Did you run the extraction script first?)")
        return None

    # 3. Load into Pandas
    try:
        df = pd.read_csv(file_path)
        print(f"✅ Successfully loaded '{filename}' with shape {df.shape}")
        return df
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return None

if __name__ == "__main__":
    # Test the function
    df = load_local_data()
    
    if df is not None:
        print(df.head())