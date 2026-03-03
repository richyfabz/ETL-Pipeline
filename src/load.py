import sqlite3
import pandas as pd


def load_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str = "users") -> None:
    """
    Load a DataFrame into a SQLite database.
    Replaces the table if it already exists.
    """
    if df.empty:
        raise ValueError("Cannot load an empty DataFrame into SQLite.")

    try:
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
        print(f"✅ Loaded {len(df)} rows into '{table_name}' table at {db_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to write to SQLite: {e}")


def query_table(db_path: str, table_name: str = "users") -> pd.DataFrame:
    """
    Helper: read the full table back from SQLite for verification.
    """
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to query SQLite: {e}")