import requests
import pandas as pd


def fetch_api_data(url: str) -> pd.DataFrame:
    """
    Fetch user data from a REST API and return as a DataFrame.
    Raises an exception if the request fails.
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # raises HTTPError for 4xx/5xx status codes
    except requests.exceptions.Timeout:
        raise RuntimeError(f"Request timed out while fetching: {url}")
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"HTTP error occurred: {e}")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch data: {e}")

    data = response.json()

    if not isinstance(data, list) or len(data) == 0:
        raise ValueError("API response is empty or not a list.")

    # Flatten only the fields we care about
    records = []
    for user in data:
        records.append({
            "id":      user.get("id"),
            "name":    user.get("name"),
            "email":   user.get("email"),
            "phone":   user.get("phone"),
            "website": user.get("website"),
            "city":    user.get("address", {}).get("city"),
        })

    return pd.DataFrame(records)


def load_csv(filepath: str) -> pd.DataFrame:
    """
    Load a local CSV file and return as a DataFrame.
    Raises an exception if the file is not found or is empty.
    """
    try:
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        raise FileNotFoundError(f"CSV file not found at: {filepath}")
    except pd.errors.EmptyDataError:
        raise ValueError(f"CSV file is empty: {filepath}")

    if df.empty:
        raise ValueError(f"CSV file loaded but contains no rows: {filepath}")

    return df

    # Temporary test block
if __name__ == "__main__":
    # Test the API fetch
    df_api = fetch_api_data("https://jsonplaceholder.typicode.com/users")
    print("API Data:")
    print(df_api)
    print()

    # Test the CSV load
    df_csv = load_csv("data/local_data.csv")
    print("CSV Data:")
    print(df_csv)