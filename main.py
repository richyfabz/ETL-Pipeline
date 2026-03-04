from src.extract import fetch_api_data, load_csv
from src.transform import clean_api_data, clean_csv_data, merge_data
from src.load import load_to_sqlite, query_table

API_URL   = "https://jsonplaceholder.typicode.com/users"
CSV_PATH  = "data/local_data.csv"
DB_PATH   = "database/etl.db"


def run_pipeline():
    print("── Extract ───────────────────────────────")
    api_df = fetch_api_data(API_URL)
    csv_df = load_csv(CSV_PATH)
    print(f"API rows fetched : {len(api_df)}")
    print(f"CSV rows loaded  : {len(csv_df)}")

    print("\n── Transform ─────────────────────────────")
    api_clean  = clean_api_data(api_df)
    csv_clean  = clean_csv_data(csv_df)
    merged     = merge_data(api_clean, csv_clean)
    print(f"Rows after merge : {len(merged)}")
    print(f"Columns          : {list(merged.columns)}")

    print("\n── Load ──────────────────────────────────")
    load_to_sqlite(merged, DB_PATH)

    print("\n── Verify ────────────────────────────────")
    result = query_table(DB_PATH)
    print(result.to_string(index=False))


if __name__ == "__main__":
    run_pipeline()