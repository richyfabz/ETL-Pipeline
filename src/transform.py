import pandas as pd


def clean_api_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the API DataFrame:
    - Drop rows missing critical fields
    - Normalize column types
    """
    required_columns = {"id", "name", "email"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"API data is missing required columns: {missing}")

    df = df.dropna(subset=["id", "name", "email"])
    df["id"] = df["id"].astype(int)
    df["email"] = df["email"].str.lower().str.strip()

    return df.reset_index(drop=True)


def clean_csv_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the CSV DataFrame:
    - Drop rows missing critical fields
    - Normalize column types
    - Validate signup_date format
    """
    required_columns = {"id", "age", "signup_date", "plan"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"CSV data is missing required columns: {missing}")

    df = df.dropna(subset=["id", "age"])
    df["id"] = df["id"].astype(int)
    df["age"] = df["age"].astype(int)
    df["plan"] = df["plan"].str.lower().str.strip()
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")

    # Drop rows where signup_date couldn't be parsed
    invalid_dates = df["signup_date"].isna().sum()
    if invalid_dates > 0:
        print(f"Warning: {invalid_dates} rows dropped due to invalid signup_date.")
        df = df.dropna(subset=["signup_date"])

    return df.reset_index(drop=True)


def merge_data(api_df: pd.DataFrame, csv_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge API and CSV data on 'id'.
    Uses an inner join — only keeps users present in BOTH sources.
    """
    merged = pd.merge(api_df, csv_df, on="id", how="inner")

    if merged.empty:
        raise ValueError("Merge produced an empty DataFrame. Check that 'id' values overlap.")

    return merged

    # Temporary test block
if __name__ == "__main__":
    import pandas as pd
    from extract import fetch_api_data, load_csv

    # Step 1 - Get the raw data
    api_df = fetch_api_data("https://jsonplaceholder.typicode.com/users")
    csv_df = load_csv("Data/local_data.csv")

    # Step 2 - Clean both
    api_clean = clean_api_data(api_df)
    csv_clean = clean_csv_data(csv_df)

    print("Cleaned API Data:")
    print(api_clean)
    print()

    print("Cleaned CSV Data:")
    print(csv_clean)
    print()

    # Step 3 - Merge
    merged = merge_data(api_clean, csv_clean)
    print("Merged Data:")
    print(merged)