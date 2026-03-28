"""Simple ETL: CSV to SQLite database"""
import pandas as pd
import sqlite3
from pathlib import Path

def extract(csv_path):
    """Extract data from CSV file."""
    print(f"Extracting from {csv_path}...")
    df = pd.read_csv(csv_path)
    print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
    return df

def transform(df):
    """Clean and transform data."""
    print("Transforming...")
    # Remove duplicates
    before = len(df)
    df = df.drop_duplicates()
    print(f"  Removed {before - len(df)} duplicates")

    # Handle missing values
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].fillna("Unknown")
    for col in df.select_dtypes(include=["number"]).columns:
        df[col] = df[col].fillna(0)

    # Normalize column names
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
    print(f"  Final shape: {df.shape}")
    return df

def load(df, db_path, table_name):
    """Load data into SQLite database."""
    print(f"Loading into {db_path} -> {table_name}...")
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print(f"  Loaded {len(df)} rows successfully!")

def run_pipeline(csv_path, db_path="output.db", table_name="data"):
    """Run the full ETL pipeline."""
    print("=" * 50)
    print("ETL Pipeline Started")
    print("=" * 50)
    df = extract(csv_path)
    df = transform(df)
    load(df, db_path, table_name)
    print("\nPipeline complete!")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python csv_to_db.py <csv_file>")
    else:
        run_pipeline(sys.argv[1])
