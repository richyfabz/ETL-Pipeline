import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.extract import fetch_api_data, load_csv
import requests as req


# ── Mock data (fake API response invented) ─────────────────────────────────

MOCK_API_RESPONSE = [
    {"id": 1, "name": "Alice", "email": "alice@example.com", "phone": "123",
     "website": "alice.com", "address": {"city": "Lagos"}},
    {"id": 2, "name": "Bob",   "email": "bob@example.com",   "phone": "456",
     "website": "bob.com",   "address": {"city": "Abuja"}},
]


# ── fetch_api_data tests ───────────────────────────────────────────────────────

def test_fetch_api_data_returns_dataframe():
    """
    ARRANGE: Fake a successful API response with 2 users
    ACT:     Call fetch_api_data with a fake URL
    ASSERT:  Result is a DataFrame with 2 rows and correct columns
    """
    with patch("src.extract.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = MOCK_API_RESPONSE
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        df = fetch_api_data("https://fake-api.com/users")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "id" in df.columns
        assert "email" in df.columns
        assert "city" in df.columns


def test_fetch_api_data_raises_on_http_error():
    """
    ARRANGE: Fake a 404 HTTP error response
    ACT:     Call fetch_api_data
    ASSERT:  RuntimeError is raised with 'HTTP error' in the message
    """
    with patch("src.extract.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = req.exceptions.HTTPError("404")
        mock_get.return_value = mock_response

        with pytest.raises(RuntimeError, match="HTTP error"):
            fetch_api_data("https://fake-api.com/users")


def test_fetch_api_data_raises_on_timeout():
    """
    ARRANGE: Fake a timeout error
    ACT:     Call fetch_api_data
    ASSERT:  RuntimeError is raised with 'timed out' in the message
    """
    with patch("src.extract.requests.get") as mock_get:
        mock_get.side_effect = req.exceptions.Timeout()

        with pytest.raises(RuntimeError, match="timed out"):
            fetch_api_data("https://fake-api.com/users")


def test_fetch_api_data_raises_on_empty_response():
    """
    ARRANGE: Fake an API that returns an empty list
    ACT:     Call fetch_api_data
    ASSERT:  ValueError is raised with 'empty' in the message
    """
    with patch("src.extract.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="empty"):
            fetch_api_data("https://fake-api.com/users")


# ── load_csv tests ─────────────────────────────────────────────────────────────

def test_load_csv_returns_dataframe(tmp_path):
    """
    ARRANGE: Create a real temporary CSV file with 2 rows
    ACT:     Call load_csv with the file path
    ASSERT:  Result is a DataFrame with correct shape and columns
    """
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("id,age,signup_date,plan\n1,28,2022-01-15,free\n2,34,2021-07-22,pro\n")

    df = load_csv(str(csv_file))

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["id", "age", "signup_date", "plan"]


def test_load_csv_raises_on_missing_file():
    """
    ARRANGE: Use a path that doesn't exist
    ACT:     Call load_csv
    ASSERT:  FileNotFoundError is raised
    """
    with pytest.raises(FileNotFoundError):
        load_csv("nonexistent/path/file.csv")


def test_load_csv_raises_on_empty_file(tmp_path):
    """
    ARRANGE: Create a real temporary CSV file with no content
    ACT:     Call load_csv
    ASSERT:  ValueError is raised
    """
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("")

    with pytest.raises(ValueError):
        load_csv(str(csv_file))