# ETL Pipeline — REST API + CSV → SQLite

A beginner-friendly but production-structured **ETL (Extract, Transform, Load)** pipeline built in Python. It pulls user data from a public REST API, merges it with a local CSV file, cleans and aligns the data, then loads the result into a SQLite database — all with a full test suite of 21 passing tests.

---

## 📌 What It Does

| Stage | Description |
|---|---|
| **Extract** | Fetches live data from [JSONPlaceholder](https://jsonplaceholder.typicode.com/users) and loads a local CSV |
| **Transform** | Cleans nulls, normalizes types/casing, parses dates, merges both sources on a shared `id` |
| **Load** | Writes the merged dataset into a local SQLite database |
| **Test** | 21 tests covering happy paths, schema validation, error handling, and SQL roundtrips |

---

## Project Structure

```
etl_project/
├── Data/
│   └── local_data.csv          # Local user data (age, plan, signup_date)
├── src/
│   ├── __init__.py
│   ├── extract.py              # Fetch from API + read CSV
│   ├── transform.py            # Clean & merge DataFrames
│   └── load.py                 # Write to SQLite
├── test/
│   ├── __init__.py
│   ├── test_extract.py         # 7 tests
│   ├── test_transform.py       # 10 tests
│   └── test_load.py            # 4 tests
├── database/
│   └── etl.db                  # SQLite DB (auto-created on first run)
├── main.py                     # Runs the full pipeline
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup

**1. Clone the repo**
```bash
git clone https://github.com/richyfabz/ETL-Pipeline.git
cd ETL-Pipeline
```

**2. Create a virtual environment (recommended)**
```bash
python3 -m venv venv
source venv/bin/activate        # macOS/Linux
venv\Scripts\activate           # Windows
```

**3. Install dependencies**
```bash
pip3 install -r requirements.txt
```

---

## 🚀 Run the Pipeline

```bash
python3 main.py
```

Expected output:
```
── Extract ───────────────────────────────
API rows fetched : 10
CSV rows loaded  : 12

── Transform ─────────────────────────────
Rows after merge : 10
Columns          : ['id', 'name', 'email', 'phone', 'website', 'city', 'age', 'signup_date', 'plan']

── Load ──────────────────────────────────
✅ Loaded 10 rows into 'users' table at database/etl.db

── Verify ────────────────────────────────
 id  name   email   ...
```

---

## Run the Tests

```bash
python3 -m pytest test/ -v
```

Expected output:
```
21 passed in 0.85s
```

Tests cover:
- API fetch success and failure (timeout, HTTP error, empty response)
- CSV loading and missing file handling
- Data cleaning — null drops, type coercion, date parsing, casing
- Merge correctness and schema alignment
- SQLite write/read roundtrip and duplicate prevention

---

## 🧰 Tech Stack

| Tool | Purpose |
|---|---|
| `requests` | HTTP requests to REST API |
| `pandas` | DataFrame manipulation and CSV I/O |
| `sqlite3` | Built-in Python SQLite interface |
| `pytest` | Test runner |
| `unittest.mock` | Mocking API calls in tests |

---

## 💡 Concepts Demonstrated

- ETL pipeline architecture
- REST API consumption with error handling
- Data cleaning and schema alignment across sources
- DataFrame merging with `pandas`
- SQLite persistence with `pandas.to_sql()`
- Unit testing with mocks (`unittest.mock.patch`)
- `pytest` fixtures for reusable test data
- Defensive programming with `try/except` blocks

---

## Data Sources

**REST API** — [JSONPlaceholder](https://jsonplaceholder.typicode.com/users)
Public fake API returning 10 users with profile data: name, email, phone, website, city.

**Local CSV** — `Data/local_data.csv`
Internal business data with user age, subscription plan, and signup date. Represents the kind of private data a company would store internally that no public API would expose.

Both sources share a common `id` column which is used to merge them into a single unified dataset.

---

## What's Next

Some ideas to extend this project:
- Add a CLI with `argparse` to pass in custom API URLs or CSV paths
- Add structured logging with Python's `logging` module
- Schedule the pipeline with `cron` or `APScheduler`
- Swap SQLite for PostgreSQL using `SQLAlchemy`
- Visualize the output data with `matplotlib` or `seaborn`
- Add a `conftest.py` to share fixtures across all test files

---

## License

MIT — free to use, modify, and build on.