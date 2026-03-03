import pytest
import pandas as pd
from src.load import load_to_sqlite, query_table


# ── Fixture ────────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_df():
    """
    Fake merged DataFrame representing what comes out of transform.py
    """
    return pd.DataFrame([
        {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 28, "plan": "free"},
        {"id": 2, "name": "Bob",   "email": "bob@example.com",   "age": 34, "plan": "pro"},
    ])


# ── load_to_sqlite tests ───────────────────────────────────────────────────────

def test_load_and_query_roundtrip(tmp_path, sample_df):
    """
    ARRANGE: Sample DataFrame and a temporary database path
    ACT:     Write to SQLite then read it back
    ASSERT:  Data read back matches what was written in
    """
    db_path = str(tmp_path / "test.db")
    load_to_sqlite(sample_df, db_path)
    result = query_table(db_path)

    assert len(result) == 2
    assert list(result.columns) == list(sample_df.columns)
    assert set(result["id"]) == {1, 2}


def test_load_creates_correct_schema(tmp_path, sample_df):
    """
    ARRANGE: Sample DataFrame with known columns
    ACT:     Write to SQLite then read it back
    ASSERT:  All expected columns exist in the database table
    """
    db_path = str(tmp_path / "test.db")
    load_to_sqlite(sample_df, db_path)
    result = query_table(db_path)

    for col in ["id", "name", "email", "age", "plan"]:
        assert col in result.columns


def test_load_replaces_existing_table(tmp_path, sample_df):
    """
    ARRANGE: Sample DataFrame loaded twice into the same database
    ACT:     Call load_to_sqlite twice
    ASSERT:  Table has 2 rows not 4 — replace worked correctly
    """
    db_path = str(tmp_path / "test.db")
    load_to_sqlite(sample_df, db_path)
    load_to_sqlite(sample_df, db_path)
    result = query_table(db_path)

    assert len(result) == 2


def test_load_raises_on_empty_dataframe(tmp_path):
    """
    ARRANGE: Completely empty DataFrame
    ACT:     Call load_to_sqlite
    ASSERT:  ValueError is raised with 'empty' in the message
    """
    db_path = str(tmp_path / "test.db")
    with pytest.raises(ValueError, match="empty"):
        load_to_sqlite(pd.DataFrame(), db_path)