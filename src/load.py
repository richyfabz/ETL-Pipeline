import sqlite3
import pandas as pd


def load_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str = "users") -> None:
    """
    Load a DataFrame into a SQLite database.
    Replaces the table if it already exists.
    Uses a context manager so the connection always closes, even on error.
    """
    if df.empty:
        raise ValueError("Cannot load an empty DataFrame into SQLite.")

    try:
        with sqlite3.connect(db_path) as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"✅ Loaded {len(df)} rows into '{table_name}' table at {db_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to write to SQLite: {e}")


def query_table(db_path: str, table_name: str = "users") -> pd.DataFrame:
    """
    Helper: read the full table back from SQLite for verification.
    NOTE: table_name is not parameterised by sqlite3 — keep it trusted/internal.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to query SQLite: {e}")