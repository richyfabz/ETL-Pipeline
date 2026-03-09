"""
main.py — ETL Pipeline Entry Point
Orchestrates: Extract → Transform → Load
"""

from extract import fetch_api_data, load_csv
from transform import clean_api_data, clean_csv_data, merge_data
from load import load_to_sqlite, query_table

#  Config 
API_URL    = "https://jsonplaceholder.typicode.com/users"
CSV_PATH   = "../Data/local_data.csv"  # your local CSV file
DB_PATH    = "etl_output.db"
TABLE_NAME = "users"


def run_pipeline():
    # 1. EXTRACT 
    print("Extracting data...")
    api_df = fetch_api_data(API_URL)
    csv_df = load_csv(CSV_PATH)
    print(f"   API rows   : {len(api_df)}")
    print(f"   CSV rows   : {len(csv_df)}")

    # 2. TRANSFORM 
    print("\n🔧 Transforming data...")
    api_clean  = clean_api_data(api_df)
    csv_clean  = clean_csv_data(csv_df)
    merged     = merge_data(api_clean, csv_clean)
    print(f"   Merged rows: {len(merged)}")
    print(f"   Columns    : {list(merged.columns)}")

    #  3. LOAD 
    print("\n Loading into SQLite...")
    load_to_sqlite(merged, DB_PATH, TABLE_NAME)

    # 4. VERIFY 
    print("\nVerifying stored data...")
    result = query_table(DB_PATH, TABLE_NAME)
    print(result.to_string(index=False))

    print("\nPipeline complete.")


if __name__ == "__main__":
    run_pipeline()