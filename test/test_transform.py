import pytest
import pandas as pd
from src.transform import clean_api_data, clean_csv_data, merge_data


# ── Fixtures (reusable sample data) ───────────────────────────────────────────

@pytest.fixture
def sample_api_df():
    """
    Fake API DataFrame with 3 rows.
    Row 3 has None values — should be dropped during cleaning.
    """
    return pd.DataFrame([
        {"id": 1, "name": "Alice", "email": "ALICE@Example.com", "phone": "123", "website": "a.com", "city": "Lagos"},
        {"id": 2, "name": "Bob",   "email": "bob@example.com",   "phone": "456", "website": "b.com", "city": "Abuja"},
        {"id": 3, "name": None,    "email": None,                "phone": None,  "website": None,    "city": None},
    ])


@pytest.fixture
def sample_csv_df():
    """
    Fake CSV DataFrame with 3 rows.
    id 4 exists here but not in the API — should be dropped during merge.
    """
    return pd.DataFrame([
        {"id": 1, "age": 28, "signup_date": "2022-01-15", "plan": "Free"},
        {"id": 2, "age": 34, "signup_date": "2021-07-22", "plan": "PRO"},
        {"id": 4, "age": 22, "signup_date": "2020-11-05", "plan": "enterprise"},
    ])


# ── clean_api_data tests ───────────────────────────────────────────────────────

def test_clean_api_drops_null_rows(sample_api_df):
    """
    ARRANGE: API DataFrame with 3 rows, row 3 has null name and email
    ACT:     Call clean_api_data
    ASSERT:  Only 2 rows remain after null drop
    """
    cleaned = clean_api_data(sample_api_df)
    assert len(cleaned) == 2


def test_clean_api_lowercases_email(sample_api_df):
    """
    ARRANGE: API DataFrame with mixed case emails
    ACT:     Call clean_api_data
    ASSERT:  All emails are lowercased
    """
    cleaned = clean_api_data(sample_api_df)
    assert all(cleaned["email"] == cleaned["email"].str.lower())


def test_clean_api_id_is_int(sample_api_df):
    """
    ARRANGE: API DataFrame with id column
    ACT:     Call clean_api_data
    ASSERT:  id column is integer type
    """
    cleaned = clean_api_data(sample_api_df)
    assert cleaned["id"].dtype == int


def test_clean_api_raises_on_missing_columns():
    """
    ARRANGE: DataFrame missing the 'email' column
    ACT:     Call clean_api_data
    ASSERT:  ValueError is raised with 'missing required columns' in message
    """
    bad_df = pd.DataFrame([{"id": 1, "name": "Alice"}])
    with pytest.raises(ValueError, match="missing required columns"):
        clean_api_data(bad_df)


# ── clean_csv_data tests ───────────────────────────────────────────────────────

def test_clean_csv_lowercases_plan(sample_csv_df):
    """
    ARRANGE: CSV DataFrame with mixed case plan values
    ACT:     Call clean_csv_data
    ASSERT:  All plan values are lowercased
    """
    cleaned = clean_csv_data(sample_csv_df)
    assert all(cleaned["plan"] == cleaned["plan"].str.lower())


def test_clean_csv_parses_dates(sample_csv_df):
    """
    ARRANGE: CSV DataFrame with signup_date as plain text strings
    ACT:     Call clean_csv_data
    ASSERT:  signup_date column is parsed into datetime type
    """
    cleaned = clean_csv_data(sample_csv_df)
    assert pd.api.types.is_datetime64_any_dtype(cleaned["signup_date"])


def test_clean_csv_raises_on_missing_columns():
    """
    ARRANGE: DataFrame missing signup_date and plan columns
    ACT:     Call clean_csv_data
    ASSERT:  ValueError is raised with 'missing required columns' in message
    """
    bad_df = pd.DataFrame([{"id": 1, "age": 25}])
    with pytest.raises(ValueError, match="missing required columns"):
        clean_csv_data(bad_df)


# ── merge_data tests ───────────────────────────────────────────────────────────

def test_merge_only_keeps_matching_ids(sample_api_df, sample_csv_df):
    """
    ARRANGE: API has ids 1,2,3 — CSV has ids 1,2,4
    ACT:     Clean both and merge
    ASSERT:  Only ids 1 and 2 remain (inner join)
    """
    api_clean = clean_api_data(sample_api_df)
    csv_clean = clean_csv_data(sample_csv_df)
    merged = merge_data(api_clean, csv_clean)

    assert set(merged["id"]) == {1, 2}


def test_merge_contains_columns_from_both_sources(sample_api_df, sample_csv_df):
    """
    ARRANGE: API and CSV DataFrames with different columns
    ACT:     Clean both and merge
    ASSERT:  Merged DataFrame contains columns from both sources
    """
    api_clean = clean_api_data(sample_api_df)
    csv_clean = clean_csv_data(sample_csv_df)
    merged = merge_data(api_clean, csv_clean)

    for col in ["name", "email", "city", "age", "plan", "signup_date"]:
        assert col in merged.columns


def test_merge_raises_on_no_overlap():
    """
    ARRANGE: API has id 1, CSV has id 99 — no matching ids
    ACT:     Clean both and merge
    ASSERT:  ValueError is raised with 'empty' in the message
    """
    api_df = pd.DataFrame([{"id": 1, "name": "Alice", "email": "a@b.com", "phone": "", "website": "", "city": ""}])
    csv_df = pd.DataFrame([{"id": 99, "age": 30, "signup_date": "2022-01-01", "plan": "free"}])

    api_clean = clean_api_data(api_df)
    csv_clean = clean_csv_data(csv_df)

    with pytest.raises(ValueError, match="empty"):
        merge_data(api_clean, csv_clean)