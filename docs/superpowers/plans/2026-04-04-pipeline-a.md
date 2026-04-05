# Pipeline A — PDF Table Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract financial data (expenditures, revenues, fund summaries) from Town of Cary PDF documents using pdfplumber/camelot and load into SQLite.

**Architecture:** PDF files in `resources/` are scanned by `page_finder.py` which locates sections by keyword (page numbers shift 10–15 pages across fiscal years, so fixed numbers are unreliable). `page_map.py` stores keyword anchors per doc type. `extractor.py` uses `extract_tables()` for quarterly reports and `extract_text()` + regex for budget/ACFR pages where table extraction fails. `normalizer.py` converts raw text/DataFrames to typed schema rows. `db.py` upserts into SQLite. Pipeline B reuses `shared/` modules.

**Tech Stack:** Python 3.12+, pdfplumber, pandas, sqlite3, pytest, uv

**Note: camelot-py[cv] dropped** — inspection confirmed `extract_table()` fails on most budget/ACFR pages anyway. Text extraction + regex is the right approach. Camelot dependency removed.

---

## File Map

```
pipelines/
  __init__.py
  pipeline_a/
    __init__.py
    page_map.py        # Keyword anchors per doc_type (NOT fixed page numbers)
    page_finder.py     # Scans PDF text to locate sections by keyword
    extractor.py       # extract_tables() for quarterly; extract_text() for budget/ACFR
    normalizer.py      # Per-doc-type normalization → schema dataclasses
    run.py             # Entry point: discover PDFs, orchestrate, log summary
  shared/
    __init__.py
    schema.py          # Expenditure, Revenue, FundSummary, NormalizedResult dataclasses
    db.py              # create_schema(), upsert_*() helpers
tests/
  conftest.py          # Shared fixtures: in-memory DB connection, sample DataFrames
  pipeline_a/
    __init__.py
    test_page_map.py
    test_extractor.py
    test_normalizer.py
    test_integration.py   # @pytest.mark.slow, hits real PDFs
data/                  # Created at runtime
  cary.db
pyproject.toml
```

---

## Task 0: Inspect PDFs to verify page ranges

The page ranges in `page_map.py` are based on the FY2025 budget TOC. ACFR and quarterly page ranges are unknown. This task establishes ground truth before any code is written.

**Files:** none (inspection only)

- [ ] **Step 1: Inspect quarterly report page ranges**

```bash
uv run python -c "
import pdfplumber
for path in ['resources/quarterly-reports/2024-q3.pdf', 'resources/quarterly-reports/2024-annual.pdf']:
    with pdfplumber.open(path) as pdf:
        print(f'\n=== {path} ({len(pdf.pages)} pages) ===')
        for i, page in enumerate(pdf.pages[:15], 1):
            text = page.extract_text() or ''
            first_line = text.strip().split('\n')[0] if text.strip() else '(no text)'
            print(f'  p{i}: {first_line[:80]}')
"
```

- [ ] **Step 2: Inspect ACFR page ranges**

```bash
uv run python -c "
import pdfplumber
with pdfplumber.open('resources/financial-reports/2024.pdf') as pdf:
    print(f'Total pages: {len(pdf.pages)}')
    for i, page in enumerate(pdf.pages, 1):
        text = page.extract_text() or ''
        first_line = text.strip().split('\n')[0] if text.strip() else '(no text)'
        print(f'  p{i}: {first_line[:80]}')
" | head -80
```

- [ ] **Step 3: Update ACFR and quarterly page ranges**

Based on output from Steps 1–2, update the `ACFR_PAGES` and `QUARTERLY_PAGES` dicts in `page_map.py` (written in Task 2) before the pipeline runs.

- [ ] **Step 4: Verify budget page ranges match FY2021**

The FY2025 TOC shows fund_summary=p65, expenditure_summary=p68, revenue_summary=p92-93, dept_profiles=p116-200. Confirm these hold for the earliest budget:

```bash
uv run python -c "
import pdfplumber
pages_to_check = [65, 68, 92, 93, 116]
with pdfplumber.open('resources/budgets/2021.pdf') as pdf:
    print(f'Total pages: {len(pdf.pages)}')
    for p in pages_to_check:
        text = pdf.pages[p-1].extract_text() or ''
        first_line = text.strip().split('\n')[0] if text.strip() else '(no text)'
        print(f'  p{p}: {first_line[:80]}')
"
```

---

## Task 1: Project setup and shared schema

**Files:**
- Create: `pyproject.toml`
- Create: `pipelines/__init__.py`
- Create: `pipelines/shared/__init__.py`
- Create: `pipelines/shared/schema.py`
- Create: `pipelines/shared/db.py`
- Create: `pipelines/pipeline_a/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Create: `tests/pipeline_a/__init__.py`
- Test: `tests/pipeline_a/test_db.py`

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[project]
name = "cary"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "pdfplumber>=0.11",
    "camelot-py[cv]>=0.11",
    "pandas>=2.2",
    "anthropic>=0.40",
    "pymupdf>=1.25",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-mock>=3.14",
]

[tool.pytest.ini_options]
markers = ["slow: marks tests as slow (deselect with '-m not slow')"]
testpaths = ["tests"]
```

- [ ] **Step 2: Install dependencies**

```bash
uv pip install -e ".[dev]"
```

Expected: packages install without error. Note: `camelot-py[cv]` installs OpenCV (~50MB). If it fails, try `camelot-py[base]` instead and update the fallback in `extractor.py` to skip camelot.

- [ ] **Step 3: Create empty `__init__.py` files**

```bash
touch pipelines/__init__.py pipelines/shared/__init__.py pipelines/pipeline_a/__init__.py tests/__init__.py tests/pipeline_a/__init__.py
```

- [ ] **Step 4: Write the failing test for schema dataclasses**

Create `tests/pipeline_a/test_db.py`:

```python
import sqlite3
from datetime import datetime, timezone
from pipelines.shared.schema import Expenditure, Revenue, FundSummary, NormalizedResult
from pipelines.shared.db import create_schema, upsert_expenditures, upsert_revenues, upsert_fund_summaries

NOW = datetime(2026, 4, 4, 12, 0, 0, tzinfo=timezone.utc)


def make_conn():
    conn = sqlite3.connect(":memory:")
    create_schema(conn)
    return conn


def make_expenditure(**kwargs) -> Expenditure:
    defaults = dict(
        pipeline="A", source_file="resources/budgets/2025.pdf",
        doc_type="budget", fiscal_year=2025, quarter=None,
        fund="General", department="Police", division=None,
        amount_type="adopted", amount=45_230_000.0, extracted_at=NOW,
    )
    return Expenditure(**{**defaults, **kwargs})


def test_create_schema_creates_tables():
    conn = make_conn()
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    table_names = {row[0] for row in tables}
    assert "expenditures" in table_names
    assert "revenues" in table_names
    assert "fund_summaries" in table_names


def test_upsert_expenditure_inserts_row():
    conn = make_conn()
    rows = [make_expenditure()]
    upsert_expenditures(conn, rows)
    count = conn.execute("SELECT COUNT(*) FROM expenditures").fetchone()[0]
    assert count == 1


def test_upsert_expenditure_is_idempotent():
    conn = make_conn()
    row = make_expenditure()
    upsert_expenditures(conn, [row])
    upsert_expenditures(conn, [row])
    count = conn.execute("SELECT COUNT(*) FROM expenditures").fetchone()[0]
    assert count == 1


def test_upsert_expenditure_updates_amount_on_conflict():
    conn = make_conn()
    upsert_expenditures(conn, [make_expenditure(amount=1_000.0)])
    upsert_expenditures(conn, [make_expenditure(amount=2_000.0)])
    amount = conn.execute("SELECT amount FROM expenditures").fetchone()[0]
    assert amount == 2_000.0


def test_upsert_different_amount_types_are_separate_rows():
    conn = make_conn()
    upsert_expenditures(conn, [
        make_expenditure(amount_type="adopted"),
        make_expenditure(amount_type="actual"),
    ])
    count = conn.execute("SELECT COUNT(*) FROM expenditures").fetchone()[0]
    assert count == 2


def test_upsert_revenues():
    conn = make_conn()
    revenue = Revenue(
        pipeline="A", source_file="resources/budgets/2025.pdf",
        doc_type="budget", fiscal_year=2025, quarter=None,
        fund="General", source="Property Tax",
        amount_type="adopted", amount=120_000_000.0, extracted_at=NOW,
    )
    upsert_revenues(conn, [revenue])
    count = conn.execute("SELECT COUNT(*) FROM revenues").fetchone()[0]
    assert count == 1


def test_upsert_revenues_is_idempotent():
    conn = make_conn()
    revenue = Revenue(
        pipeline="A", source_file="resources/budgets/2025.pdf",
        doc_type="budget", fiscal_year=2025, quarter=None,
        fund="General", source="Property Tax",
        amount_type="adopted", amount=120_000_000.0, extracted_at=NOW,
    )
    upsert_revenues(conn, [revenue])
    upsert_revenues(conn, [revenue])
    count = conn.execute("SELECT COUNT(*) FROM revenues").fetchone()[0]
    assert count == 1


def test_upsert_fund_summary():
    conn = make_conn()
    summary = FundSummary(
        pipeline="A", source_file="resources/budgets/2025.pdf",
        doc_type="budget", fiscal_year=2025, quarter=None,
        fund="General", total_revenues=200_000_000.0,
        total_expenditures=195_000_000.0, transfers_in=5_000_000.0,
        transfers_out=10_000_000.0, beginning_balance=50_000_000.0,
        ending_balance=50_000_000.0, extracted_at=NOW,
    )
    upsert_fund_summaries(conn, [summary])
    count = conn.execute("SELECT COUNT(*) FROM fund_summaries").fetchone()[0]
    assert count == 1
```

- [ ] **Step 5: Run tests — verify they fail**

```bash
uv run pytest tests/pipeline_a/test_db.py -v
```

Expected: `ModuleNotFoundError: No module named 'pipelines'`

- [ ] **Step 6: Write `pipelines/shared/schema.py`**

```python
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Expenditure:
    pipeline: str
    source_file: str
    doc_type: str
    fiscal_year: int
    quarter: int | None
    fund: str
    department: str
    division: str | None
    amount_type: str
    amount: float
    extracted_at: datetime


@dataclass
class Revenue:
    pipeline: str
    source_file: str
    doc_type: str
    fiscal_year: int
    quarter: int | None
    fund: str
    source: str
    amount_type: str
    amount: float
    extracted_at: datetime


@dataclass
class FundSummary:
    pipeline: str
    source_file: str
    doc_type: str
    fiscal_year: int
    quarter: int | None
    fund: str
    total_revenues: float | None
    total_expenditures: float | None
    transfers_in: float | None
    transfers_out: float | None
    beginning_balance: float | None
    ending_balance: float | None
    extracted_at: datetime


@dataclass
class NormalizedResult:
    expenditures: list[Expenditure]
    revenues: list[Revenue]
    fund_summaries: list[FundSummary]
```

- [ ] **Step 7: Write `pipelines/shared/db.py`**

```python
import sqlite3
from pathlib import Path
from .schema import Expenditure, Revenue, FundSummary

DB_PATH = Path("data/cary.db")

_CREATE_EXPENDITURES = """
CREATE TABLE IF NOT EXISTS expenditures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline TEXT NOT NULL,
    source_file TEXT NOT NULL,
    doc_type TEXT NOT NULL,
    fiscal_year INTEGER NOT NULL,
    quarter INTEGER,
    fund TEXT NOT NULL,
    department TEXT NOT NULL,
    division TEXT,
    amount_type TEXT NOT NULL,
    amount REAL NOT NULL,
    extracted_at TEXT NOT NULL,
    UNIQUE(pipeline, source_file, doc_type, fiscal_year, quarter,
           fund, department, division, amount_type)
)
"""

_CREATE_REVENUES = """
CREATE TABLE IF NOT EXISTS revenues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline TEXT NOT NULL,
    source_file TEXT NOT NULL,
    doc_type TEXT NOT NULL,
    fiscal_year INTEGER NOT NULL,
    quarter INTEGER,
    fund TEXT NOT NULL,
    source TEXT NOT NULL,
    amount_type TEXT NOT NULL,
    amount REAL NOT NULL,
    extracted_at TEXT NOT NULL,
    UNIQUE(pipeline, source_file, doc_type, fiscal_year, quarter,
           fund, source, amount_type)
)
"""

_CREATE_FUND_SUMMARIES = """
CREATE TABLE IF NOT EXISTS fund_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline TEXT NOT NULL,
    source_file TEXT NOT NULL,
    doc_type TEXT NOT NULL,
    fiscal_year INTEGER NOT NULL,
    quarter INTEGER,
    fund TEXT NOT NULL,
    total_revenues REAL,
    total_expenditures REAL,
    transfers_in REAL,
    transfers_out REAL,
    beginning_balance REAL,
    ending_balance REAL,
    extracted_at TEXT NOT NULL,
    UNIQUE(pipeline, source_file, doc_type, fiscal_year, quarter, fund)
)
"""


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    conn.execute(_CREATE_EXPENDITURES)
    conn.execute(_CREATE_REVENUES)
    conn.execute(_CREATE_FUND_SUMMARIES)
    conn.commit()


def upsert_expenditures(conn: sqlite3.Connection, rows: list[Expenditure]) -> int:
    sql = """
    INSERT INTO expenditures
        (pipeline, source_file, doc_type, fiscal_year, quarter, fund,
         department, division, amount_type, amount, extracted_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(pipeline, source_file, doc_type, fiscal_year, quarter,
                fund, department, division, amount_type)
    DO UPDATE SET amount=excluded.amount, extracted_at=excluded.extracted_at
    """
    conn.executemany(sql, [
        (r.pipeline, r.source_file, r.doc_type, r.fiscal_year, r.quarter,
         r.fund, r.department, r.division, r.amount_type, r.amount,
         r.extracted_at.isoformat())
        for r in rows
    ])
    conn.commit()
    return len(rows)


def upsert_revenues(conn: sqlite3.Connection, rows: list[Revenue]) -> int:
    sql = """
    INSERT INTO revenues
        (pipeline, source_file, doc_type, fiscal_year, quarter, fund,
         source, amount_type, amount, extracted_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(pipeline, source_file, doc_type, fiscal_year, quarter,
                fund, source, amount_type)
    DO UPDATE SET amount=excluded.amount, extracted_at=excluded.extracted_at
    """
    conn.executemany(sql, [
        (r.pipeline, r.source_file, r.doc_type, r.fiscal_year, r.quarter,
         r.fund, r.source, r.amount_type, r.amount, r.extracted_at.isoformat())
        for r in rows
    ])
    conn.commit()
    return len(rows)


def upsert_fund_summaries(conn: sqlite3.Connection, rows: list[FundSummary]) -> int:
    sql = """
    INSERT INTO fund_summaries
        (pipeline, source_file, doc_type, fiscal_year, quarter, fund,
         total_revenues, total_expenditures, transfers_in, transfers_out,
         beginning_balance, ending_balance, extracted_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(pipeline, source_file, doc_type, fiscal_year, quarter, fund)
    DO UPDATE SET
        total_revenues=excluded.total_revenues,
        total_expenditures=excluded.total_expenditures,
        transfers_in=excluded.transfers_in,
        transfers_out=excluded.transfers_out,
        beginning_balance=excluded.beginning_balance,
        ending_balance=excluded.ending_balance,
        extracted_at=excluded.extracted_at
    """
    conn.executemany(sql, [
        (r.pipeline, r.source_file, r.doc_type, r.fiscal_year, r.quarter, r.fund,
         r.total_revenues, r.total_expenditures, r.transfers_in, r.transfers_out,
         r.beginning_balance, r.ending_balance, r.extracted_at.isoformat())
        for r in rows
    ])
    conn.commit()
    return len(rows)
```

- [ ] **Step 8: Run tests — verify they pass**

```bash
uv run pytest tests/pipeline_a/test_db.py -v
```

Expected: all 9 tests PASS

- [ ] **Step 9: Commit**

```bash
git add pyproject.toml pipelines/ tests/
git commit -m "feat: shared schema, db helpers, and pipeline_a scaffold"
```

---

## Task 2: `page_map.py`

**Files:**
- Create: `pipelines/pipeline_a/page_map.py`
- Test: `tests/pipeline_a/test_page_map.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/pipeline_a/test_page_map.py`:

```python
import pytest
from pipelines.pipeline_a.page_map import get_pages


def test_budget_2025_returns_expected_sections():
    pages = get_pages("budget", 2025)
    assert "fund_summary" in pages
    assert "expenditure_summary" in pages
    assert "revenue_summary" in pages
    assert "dept_profiles" in pages


def test_budget_2025_expenditure_summary_is_page_68():
    pages = get_pages("budget", 2025)
    assert pages["expenditure_summary"] == [68, 68]


def test_budget_2025_dept_profiles_start_at_116():
    pages = get_pages("budget", 2025)
    start, end = pages["dept_profiles"]
    assert start == 116
    assert end > start


def test_all_budget_years_have_page_maps():
    for year in [2021, 2022, 2023, 2024, 2025, 2026]:
        pages = get_pages("budget", year)
        assert isinstance(pages, dict), f"Missing budget page map for {year}"


def test_all_acfr_years_have_page_maps():
    for year in [2019, 2020, 2021, 2022, 2023, 2024, 2025]:
        pages = get_pages("acfr", year)
        assert isinstance(pages, dict), f"Missing ACFR page map for {year}"


def test_quarterly_returns_financial_highlights():
    pages = get_pages("quarterly", 2024)
    assert "financial_highlights" in pages


def test_unknown_doc_type_raises_value_error():
    with pytest.raises(ValueError, match="Unknown doc_type"):
        get_pages("unknown", 2025)


def test_unknown_budget_year_raises_value_error():
    with pytest.raises(ValueError, match="No page map for budget fiscal year 1999"):
        get_pages("budget", 1999)
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
uv run pytest tests/pipeline_a/test_page_map.py -v
```

Expected: `ModuleNotFoundError` or `ImportError`

- [ ] **Step 3: Write `pipelines/pipeline_a/page_map.py`**

Fill in ACFR and quarterly ranges based on your inspection findings from Task 0.

```python
# Page ranges are 1-indexed, inclusive [start, end].
# Budget ranges are based on FY2025 TOC. Verify others by inspection.

_BUDGET_PAGES: dict[int, dict[str, list[int]]] = {
    year: {
        "fund_summary": [65, 65],
        "expenditure_summary": [68, 68],
        "revenue_summary": [92, 93],
        "dept_profiles": [116, 200],
    }
    for year in [2021, 2022, 2023, 2024, 2025, 2026]
}

# IMPORTANT: Verify these ranges using Task 0 inspection before running the pipeline.
# These are placeholders based on typical ACFR structure.
_ACFR_PAGES: dict[int, dict[str, list[int]]] = {
    year: {
        "financial_statements": [20, 60],
    }
    for year in [2019, 2020, 2021, 2022, 2023, 2024, 2025]
}

# IMPORTANT: Verify using Task 0 inspection.
_QUARTERLY_PAGES: dict[str, list[int]] = {
    "financial_highlights": [5, 12],
}


def get_pages(doc_type: str, fiscal_year: int) -> dict[str, list[int]]:
    """
    Return the page ranges for a document type and fiscal year.
    Keys are section names; values are [start_page, end_page] (1-indexed, inclusive).
    """
    if doc_type == "budget":
        if fiscal_year not in _BUDGET_PAGES:
            raise ValueError(
                f"No page map for budget fiscal year {fiscal_year}. "
                "Add it to pipelines/pipeline_a/page_map.py."
            )
        return _BUDGET_PAGES[fiscal_year]
    elif doc_type == "acfr":
        if fiscal_year not in _ACFR_PAGES:
            raise ValueError(
                f"No page map for ACFR fiscal year {fiscal_year}. "
                "Add it to pipelines/pipeline_a/page_map.py."
            )
        return _ACFR_PAGES[fiscal_year]
    elif doc_type == "quarterly":
        return _QUARTERLY_PAGES
    else:
        raise ValueError(
            f"Unknown doc_type: {doc_type!r}. Expected 'budget', 'acfr', or 'quarterly'."
        )
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
uv run pytest tests/pipeline_a/test_page_map.py -v
```

Expected: all 8 tests PASS

- [ ] **Step 5: Commit**

```bash
git add pipelines/pipeline_a/page_map.py tests/pipeline_a/test_page_map.py
git commit -m "feat: pipeline_a page_map with TDD"
```

---

## Task 3: `extractor.py`

**Files:**
- Create: `pipelines/pipeline_a/extractor.py`
- Test: `tests/pipeline_a/test_extractor.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/pipeline_a/test_extractor.py`:

```python
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from pipelines.pipeline_a.extractor import extract_tables


def _make_mock_page(table_data: list[list] | None):
    """Return a mock pdfplumber page."""
    page = MagicMock()
    page.extract_table.return_value = table_data
    return page


def _make_mock_pdf(pages_data: list[list[list] | None]):
    """Return a mock pdfplumber PDF with given pages."""
    mock_pdf = MagicMock()
    mock_pdf.__enter__ = lambda s: s
    mock_pdf.__exit__ = MagicMock(return_value=False)
    mock_pdf.pages = [_make_mock_page(d) for d in pages_data]
    return mock_pdf


def test_extract_tables_returns_dataframe_for_valid_table():
    table_data = [
        ["Department", "FY 2023 Actual", "FY 2025 Adopted"],
        ["Police", "43,000,000", "45,230,000"],
        ["Parks", "12,000,000", "13,500,000"],
    ]
    mock_pdf = _make_mock_pdf([None, None, None, None, None,
                               None, None, None, None, None,
                               None, None, None, None, None,
                               None, None, None, None, None,
                               None, None, None, None, None,
                               None, None, None, None, None,
                               None, None, None, None, None,
                               None, None, None, None, None,
                               None, None, None, None, None,
                               None, None, None, None, None,
                               None, None, None, None, None,
                               None, None, None, None, None,
                               None, None, None, None, None,
                               None, None, None, table_data])
    # page 68 is index 67
    pages_list = [None] * 67 + [table_data]
    mock_pdf.pages = [_make_mock_page(d) for d in pages_list]

    with patch("pdfplumber.open", return_value=mock_pdf):
        results = extract_tables("fake.pdf", [68])

    assert len(results) == 1
    page_num, df = results[0]
    assert page_num == 68
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["Department", "FY 2023 Actual", "FY 2025 Adopted"]
    assert len(df) == 2


def test_extract_tables_skips_page_with_no_table():
    pages_list = [None] * 67 + [None]
    mock_pdf = _make_mock_pdf(pages_list)
    mock_pdf.pages = [_make_mock_page(d) for d in pages_list]

    with patch("pdfplumber.open", return_value=mock_pdf):
        with patch("pipelines.pipeline_a.extractor._extract_with_camelot", return_value=None):
            results = extract_tables("fake.pdf", [68])

    assert results == []


def test_extract_tables_falls_back_to_camelot_when_pdfplumber_returns_none():
    pages_list = [None] * 67 + [None]
    mock_pdf = _make_mock_pdf(pages_list)
    mock_pdf.pages = [_make_mock_page(d) for d in pages_list]

    camelot_df = pd.DataFrame([["Police", "45000000"]], columns=["Department", "FY 2025 Adopted"])

    with patch("pdfplumber.open", return_value=mock_pdf):
        with patch("pipelines.pipeline_a.extractor._extract_with_camelot", return_value=camelot_df) as mock_camelot:
            results = extract_tables("fake.pdf", [68])

    mock_camelot.assert_called_once_with("fake.pdf", 68)
    assert len(results) == 1


def test_extract_tables_continues_after_page_error():
    table_data = [
        ["Department", "FY 2025 Adopted"],
        ["Police", "45,230,000"],
    ]
    # page 68 raises, page 92 succeeds
    page_67 = MagicMock()
    page_67.extract_table.side_effect = Exception("PDF parse error")
    page_91 = MagicMock()
    page_91.extract_table.return_value = table_data

    pages_list = [MagicMock()] * 67 + [page_67] + [MagicMock()] * 23 + [page_91]
    for p in pages_list:
        if p is not page_67 and p is not page_91:
            p.extract_table.return_value = None

    mock_pdf = MagicMock()
    mock_pdf.__enter__ = lambda s: s
    mock_pdf.__exit__ = MagicMock(return_value=False)
    mock_pdf.pages = pages_list

    with patch("pdfplumber.open", return_value=mock_pdf):
        with patch("pipelines.pipeline_a.extractor._extract_with_camelot", return_value=None):
            results = extract_tables("fake.pdf", [68, 92])

    # page 68 failed (logged), page 92 succeeded
    assert len(results) == 1
    assert results[0][0] == 92


def test_extract_tables_returns_multiple_pages():
    table_a = [["Dept", "Amount"], ["Police", "45000000"]]
    table_b = [["Source", "Amount"], ["Property Tax", "120000000"]]
    pages_list = [None] * 91 + [table_a] + [table_b]
    mock_pdf = _make_mock_pdf(pages_list)
    mock_pdf.pages = [_make_mock_page(d) for d in pages_list]

    with patch("pdfplumber.open", return_value=mock_pdf):
        results = extract_tables("fake.pdf", [92, 93])

    assert len(results) == 2
    assert results[0][0] == 92
    assert results[1][0] == 93
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
uv run pytest tests/pipeline_a/test_extractor.py -v
```

Expected: `ModuleNotFoundError`

- [ ] **Step 3: Write `pipelines/pipeline_a/extractor.py`**

```python
import logging
import pandas as pd
import pdfplumber

logger = logging.getLogger(__name__)


def _extract_with_pdfplumber(pdf_path: str, page_number: int) -> pd.DataFrame | None:
    """Extract table from a single page (1-indexed) using pdfplumber."""
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[page_number - 1]
        table = page.extract_table()
    if not table or len(table) < 2:
        return None
    df = pd.DataFrame(table[1:], columns=table[0])
    if df.shape[1] <= 1:
        return None
    return df


def _extract_with_camelot(pdf_path: str, page_number: int) -> pd.DataFrame | None:
    """Fallback extraction using camelot lattice mode."""
    try:
        import camelot
        tables = camelot.read_pdf(pdf_path, pages=str(page_number), flavor="lattice")
        if not tables:
            return None
        return tables[0].df
    except Exception as e:
        logger.warning(f"camelot fallback failed for page {page_number}: {e}")
        return None


def extract_tables(pdf_path: str, pages: list[int]) -> list[tuple[int, pd.DataFrame]]:
    """
    Extract tables from the specified 1-indexed pages of a PDF.
    Returns [(page_number, dataframe), ...]. Pages with no table are omitted.
    Errors on individual pages are logged and skipped.
    """
    results = []
    for page_num in pages:
        try:
            df = _extract_with_pdfplumber(pdf_path, page_num)
            if df is None:
                logger.warning(
                    f"pdfplumber found no table on p{page_num} of {pdf_path}, trying camelot"
                )
                df = _extract_with_camelot(pdf_path, page_num)
            if df is None:
                logger.warning(f"No table found on p{page_num} of {pdf_path}")
                continue
            results.append((page_num, df))
        except Exception as e:
            logger.error(f"Failed to extract p{page_num} from {pdf_path}: {e}")
    return results
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
uv run pytest tests/pipeline_a/test_extractor.py -v
```

Expected: all 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add pipelines/pipeline_a/extractor.py tests/pipeline_a/test_extractor.py
git commit -m "feat: pipeline_a extractor with pdfplumber/camelot and TDD"
```

---

## Task 4: `normalizer.py` — budget sections

**Files:**
- Create: `pipelines/pipeline_a/normalizer.py`
- Test: `tests/pipeline_a/test_normalizer.py`

- [ ] **Step 1: Write failing tests for `_parse_amount` and budget normalizer**

Create `tests/pipeline_a/test_normalizer.py`:

```python
import pandas as pd
import pytest
from datetime import datetime, timezone
from pipelines.pipeline_a.normalizer import (
    _parse_amount,
    normalize_budget,
    normalize_quarterly,
    normalize_acfr,
)
from pipelines.shared.schema import NormalizedResult

NOW = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
SOURCE = "resources/budgets/2025.pdf"


# --- _parse_amount ---

def test_parse_amount_plain_integer():
    assert _parse_amount("45230000") == 45_230_000.0

def test_parse_amount_with_commas():
    assert _parse_amount("45,230,000") == 45_230_000.0

def test_parse_amount_with_dollar_sign():
    assert _parse_amount("$45,230,000") == 45_230_000.0

def test_parse_amount_with_accounting_negative():
    assert _parse_amount("(1,000)") == -1_000.0

def test_parse_amount_dash_returns_none():
    assert _parse_amount("-") is None

def test_parse_amount_empty_returns_none():
    assert _parse_amount("") is None

def test_parse_amount_none_returns_none():
    assert _parse_amount(None) is None


# --- normalize_budget: expenditure_summary ---

def _make_expenditure_df() -> pd.DataFrame:
    return pd.DataFrame([
        ["Police", "43,000,000", "44,000,000", "45,230,000"],
        ["Parks & Recreation", "12,000,000", "13,000,000", "13,500,000"],
        ["Total", "55,000,000", "57,000,000", "58,730,000"],
    ], columns=["Department", "FY 2023 Actual", "FY 2024 Adopted", "FY 2025 Adopted"])


def test_normalize_budget_expenditure_summary_returns_expenditures():
    section_tables = {"expenditure_summary": [(68, _make_expenditure_df())]}
    result = normalize_budget(section_tables, SOURCE, 2025)
    assert isinstance(result, NormalizedResult)
    depts = {e.department for e in result.expenditures}
    assert "Police" in depts
    assert "Parks & Recreation" in depts


def test_normalize_budget_drops_total_rows():
    section_tables = {"expenditure_summary": [(68, _make_expenditure_df())]}
    result = normalize_budget(section_tables, SOURCE, 2025)
    depts = {e.department for e in result.expenditures}
    assert "Total" not in depts


def test_normalize_budget_correct_amount_types():
    section_tables = {"expenditure_summary": [(68, _make_expenditure_df())]}
    result = normalize_budget(section_tables, SOURCE, 2025)
    amount_types = {e.amount_type for e in result.expenditures}
    assert "actual" in amount_types
    assert "adopted" in amount_types


def test_normalize_budget_correct_amounts():
    section_tables = {"expenditure_summary": [(68, _make_expenditure_df())]}
    result = normalize_budget(section_tables, SOURCE, 2025)
    police_adopted = next(
        e.amount for e in result.expenditures
        if e.department == "Police" and e.amount_type == "adopted"
    )
    assert police_adopted == 45_230_000.0


def test_normalize_budget_pipeline_is_A():
    section_tables = {"expenditure_summary": [(68, _make_expenditure_df())]}
    result = normalize_budget(section_tables, SOURCE, 2025)
    assert all(e.pipeline == "A" for e in result.expenditures)


def test_normalize_budget_fiscal_year_is_correct():
    section_tables = {"expenditure_summary": [(68, _make_expenditure_df())]}
    result = normalize_budget(section_tables, SOURCE, 2025)
    assert all(e.fiscal_year == 2025 for e in result.expenditures)


def test_normalize_budget_quarter_is_none():
    section_tables = {"expenditure_summary": [(68, _make_expenditure_df())]}
    result = normalize_budget(section_tables, SOURCE, 2025)
    assert all(e.quarter is None for e in result.expenditures)


# --- normalize_budget: revenue_summary ---

def _make_revenue_df() -> pd.DataFrame:
    return pd.DataFrame([
        ["Property Tax", "110,000,000", "115,000,000", "120,000,000"],
        ["Sales Tax", "30,000,000", "32,000,000", "34,000,000"],
        ["Total", "140,000,000", "147,000,000", "154,000,000"],
    ], columns=["Source", "FY 2023 Actual", "FY 2024 Adopted", "FY 2025 Adopted"])


def test_normalize_budget_revenue_summary_returns_revenues():
    section_tables = {"revenue_summary": [(92, _make_revenue_df())]}
    result = normalize_budget(section_tables, SOURCE, 2025)
    sources = {r.source for r in result.revenues}
    assert "Property Tax" in sources
    assert "Sales Tax" in sources


def test_normalize_budget_revenue_drops_total_rows():
    section_tables = {"revenue_summary": [(92, _make_revenue_df())]}
    result = normalize_budget(section_tables, SOURCE, 2025)
    sources = {r.source for r in result.revenues}
    assert "Total" not in sources


# --- normalize_quarterly ---

def _make_quarterly_df() -> pd.DataFrame:
    return pd.DataFrame([
        ["General Fund", "150,000,000", "130,000,000", "200,000,000"],
        ["Utility Fund", "50,000,000", "45,000,000", "70,000,000"],
    ], columns=["Fund", "YTD Actual", "YTD Budget", "Annual Budget"])


def test_normalize_quarterly_returns_fund_summaries():
    section_tables = {"financial_highlights": [(7, _make_quarterly_df())]}
    result = normalize_quarterly(section_tables, "resources/quarterly-reports/2024-q3.pdf", 2024, 3)
    assert isinstance(result, NormalizedResult)


def test_normalize_quarterly_sets_quarter():
    section_tables = {"financial_highlights": [(7, _make_quarterly_df())]}
    result = normalize_quarterly(section_tables, "resources/quarterly-reports/2024-q3.pdf", 2024, 3)
    assert all(fs.quarter == 3 for fs in result.fund_summaries)


# --- normalize_acfr ---

def _make_acfr_df() -> pd.DataFrame:
    return pd.DataFrame([
        ["General Fund", "200,000,000", "195,000,000", "5,000,000", "10,000,000", "50,000,000", "50,000,000"],
    ], columns=["Fund", "Total Revenues", "Total Expenditures", "Transfers In", "Transfers Out", "Beginning Balance", "Ending Balance"])


def test_normalize_acfr_returns_fund_summaries():
    section_tables = {"financial_statements": [(25, _make_acfr_df())]}
    result = normalize_acfr(section_tables, "resources/financial-reports/2024.pdf", 2024)
    assert isinstance(result, NormalizedResult)
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
uv run pytest tests/pipeline_a/test_normalizer.py -v
```

Expected: `ModuleNotFoundError`

- [ ] **Step 3: Write `pipelines/pipeline_a/normalizer.py`**

```python
import logging
import re
from datetime import datetime, timezone

import pandas as pd

from pipelines.shared.schema import Expenditure, FundSummary, NormalizedResult, Revenue

logger = logging.getLogger(__name__)

_SKIP_NAMES = {"total", "subtotal", "grand total", ""}


def _parse_amount(value: str | None) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if s in ("-", "—", ""):
        return None
    negative = s.startswith("(") and s.endswith(")")
    cleaned = re.sub(r"[$,\s()]", "", s)
    try:
        result = float(cleaned)
        return -result if negative else result
    except ValueError:
        return None


def _infer_amount_type(col: str) -> str | None:
    col_lower = col.lower()
    if "actual" in col_lower:
        return "actual"
    if "adopted" in col_lower:
        return "adopted"
    if "recommended" in col_lower:
        return "recommended"
    if "prior year" in col_lower:
        return "prior_year_actual"
    return None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_budget(
    section_tables: dict[str, list[tuple[int, pd.DataFrame]]],
    source_file: str,
    fiscal_year: int,
) -> NormalizedResult:
    expenditures: list[Expenditure] = []
    revenues: list[Revenue] = []
    fund_summaries: list[FundSummary] = []
    now = _now()

    for section, tables in section_tables.items():
        for page_num, df in tables:
            if df.empty or df.shape[1] < 2:
                continue
            cols = list(df.columns)
            name_col = cols[0]
            amount_cols = [(c, _infer_amount_type(c)) for c in cols[1:] if _infer_amount_type(c)]

            for _, row in df.iterrows():
                name = str(row[name_col]).strip() if pd.notna(row[name_col]) else ""
                if name.lower() in _SKIP_NAMES:
                    continue

                for col_name, amount_type in amount_cols:
                    amount = _parse_amount(row.get(col_name))
                    if amount is None:
                        continue

                    if section in ("expenditure_summary", "dept_profiles"):
                        expenditures.append(Expenditure(
                            pipeline="A",
                            source_file=source_file,
                            doc_type="budget",
                            fiscal_year=fiscal_year,
                            quarter=None,
                            fund="General",
                            department=name,
                            division=None,
                            amount_type=amount_type,
                            amount=amount,
                            extracted_at=now,
                        ))
                    elif section in ("revenue_summary",):
                        revenues.append(Revenue(
                            pipeline="A",
                            source_file=source_file,
                            doc_type="budget",
                            fiscal_year=fiscal_year,
                            quarter=None,
                            fund="General",
                            source=name,
                            amount_type=amount_type,
                            amount=amount,
                            extracted_at=now,
                        ))
                    elif section == "fund_summary":
                        # Fund summary rows have fund name + total columns
                        fund_summaries.append(FundSummary(
                            pipeline="A",
                            source_file=source_file,
                            doc_type="budget",
                            fiscal_year=fiscal_year,
                            quarter=None,
                            fund=name,
                            total_revenues=None,
                            total_expenditures=amount if amount_type == "adopted" else None,
                            transfers_in=None,
                            transfers_out=None,
                            beginning_balance=None,
                            ending_balance=None,
                            extracted_at=now,
                        ))

    return NormalizedResult(
        expenditures=expenditures,
        revenues=revenues,
        fund_summaries=fund_summaries,
    )


def normalize_quarterly(
    section_tables: dict[str, list[tuple[int, pd.DataFrame]]],
    source_file: str,
    fiscal_year: int,
    quarter: int | None,
) -> NormalizedResult:
    expenditures: list[Expenditure] = []
    revenues: list[Revenue] = []
    fund_summaries: list[FundSummary] = []
    now = _now()

    for section, tables in section_tables.items():
        for page_num, df in tables:
            if df.empty or df.shape[1] < 2:
                continue
            cols = list(df.columns)
            name_col = cols[0]

            for _, row in df.iterrows():
                name = str(row[name_col]).strip() if pd.notna(row[name_col]) else ""
                if name.lower() in _SKIP_NAMES:
                    continue

                # Map column names to fund summary fields
                col_map = {c.lower(): c for c in cols[1:]}
                def get_col(keyword: str) -> float | None:
                    for k, c in col_map.items():
                        if keyword in k:
                            return _parse_amount(row.get(c))
                    return None

                fund_summaries.append(FundSummary(
                    pipeline="A",
                    source_file=source_file,
                    doc_type="quarterly",
                    fiscal_year=fiscal_year,
                    quarter=quarter,
                    fund=name,
                    total_revenues=get_col("revenue"),
                    total_expenditures=get_col("actual") or get_col("expenditure"),
                    transfers_in=None,
                    transfers_out=None,
                    beginning_balance=None,
                    ending_balance=None,
                    extracted_at=now,
                ))

    return NormalizedResult(
        expenditures=expenditures,
        revenues=revenues,
        fund_summaries=fund_summaries,
    )


def normalize_acfr(
    section_tables: dict[str, list[tuple[int, pd.DataFrame]]],
    source_file: str,
    fiscal_year: int,
) -> NormalizedResult:
    expenditures: list[Expenditure] = []
    revenues: list[Revenue] = []
    fund_summaries: list[FundSummary] = []
    now = _now()

    for section, tables in section_tables.items():
        for page_num, df in tables:
            if df.empty or df.shape[1] < 2:
                continue
            cols = list(df.columns)
            name_col = cols[0]
            col_lower = {c.lower(): c for c in cols[1:]}

            def get_col(keyword: str) -> str | None:
                for k, c in col_lower.items():
                    if keyword in k:
                        return c
                return None

            for _, row in df.iterrows():
                name = str(row[name_col]).strip() if pd.notna(row[name_col]) else ""
                if name.lower() in _SKIP_NAMES:
                    continue

                def amt(keyword: str) -> float | None:
                    col = get_col(keyword)
                    return _parse_amount(row.get(col)) if col else None

                fund_summaries.append(FundSummary(
                    pipeline="A",
                    source_file=source_file,
                    doc_type="acfr",
                    fiscal_year=fiscal_year,
                    quarter=None,
                    fund=name,
                    total_revenues=amt("revenue"),
                    total_expenditures=amt("expenditure"),
                    transfers_in=amt("transfers in"),
                    transfers_out=amt("transfers out"),
                    beginning_balance=amt("beginning"),
                    ending_balance=amt("ending"),
                    extracted_at=now,
                ))

    return NormalizedResult(
        expenditures=expenditures,
        revenues=revenues,
        fund_summaries=fund_summaries,
    )
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
uv run pytest tests/pipeline_a/test_normalizer.py -v
```

Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
git add pipelines/pipeline_a/normalizer.py tests/pipeline_a/test_normalizer.py
git commit -m "feat: pipeline_a normalizer for budget/quarterly/acfr with TDD"
```

---

## Task 5: `run.py` and integration test

**Files:**
- Create: `pipelines/pipeline_a/run.py`
- Test: `tests/pipeline_a/test_integration.py`

- [ ] **Step 1: Write `pipelines/pipeline_a/run.py`**

```python
"""
Pipeline A entry point.

Usage:
    uv run pipelines/pipeline_a/run.py
    uv run pipelines/pipeline_a/run.py --file resources/budgets/2025.pdf
"""
import argparse
import logging
import sys
from pathlib import Path

from pipelines.pipeline_a.extractor import extract_tables
from pipelines.pipeline_a.normalizer import normalize_budget, normalize_quarterly, normalize_acfr
from pipelines.pipeline_a.page_map import get_pages
from pipelines.shared.db import create_schema, get_connection, upsert_expenditures, upsert_revenues, upsert_fund_summaries

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RESOURCE_DIRS = {
    "budgets": "budget",
    "financial-reports": "acfr",
    "quarterly-reports": "quarterly",
}


def parse_filename(path: Path) -> tuple[str, int, int | None]:
    """Return (doc_type, fiscal_year, quarter) from resource path."""
    doc_type = RESOURCE_DIRS.get(path.parent.name)
    if doc_type is None:
        raise ValueError(f"Unknown resource directory: {path.parent.name}")
    stem = path.stem
    if doc_type == "quarterly":
        if "-q" in stem:
            year_str, q_str = stem.split("-q", 1)
            return doc_type, int(year_str), int(q_str)
        elif stem.endswith("-annual"):
            return doc_type, int(stem.removesuffix("-annual")), None
        else:
            raise ValueError(f"Cannot parse quarterly filename: {stem}")
    return doc_type, int(stem), None


def process_file(pdf_path: Path, conn) -> dict:
    """Process a single PDF. Returns summary dict."""
    doc_type, fiscal_year, quarter = parse_filename(pdf_path)
    page_map = get_pages(doc_type, fiscal_year)

    section_tables: dict = {}
    for section, (start, end) in page_map.items():
        pages = list(range(start, end + 1))
        tables = extract_tables(str(pdf_path), pages)
        if tables:
            section_tables[section] = tables

    if doc_type == "budget":
        result = normalize_budget(section_tables, str(pdf_path), fiscal_year)
    elif doc_type == "acfr":
        result = normalize_acfr(section_tables, str(pdf_path), fiscal_year)
    elif doc_type == "quarterly":
        result = normalize_quarterly(section_tables, str(pdf_path), fiscal_year, quarter)
    else:
        raise ValueError(f"Unknown doc_type: {doc_type}")

    n_exp = upsert_expenditures(conn, result.expenditures)
    n_rev = upsert_revenues(conn, result.revenues)
    n_fs = upsert_fund_summaries(conn, result.fund_summaries)

    return {"expenditures": n_exp, "revenues": n_rev, "fund_summaries": n_fs}


def discover_pdfs(resource_root: Path = Path("resources")) -> list[Path]:
    return sorted(resource_root.rglob("*.pdf"))


def main(args: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Pipeline A: PDF table extraction")
    parser.add_argument("--file", help="Process a single PDF file")
    parsed = parser.parse_args(args)

    conn = get_connection()
    create_schema(conn)

    pdfs = [Path(parsed.file)] if parsed.file else discover_pdfs()
    totals = {"expenditures": 0, "revenues": 0, "fund_summaries": 0, "errors": 0}

    for pdf_path in pdfs:
        logger.info(f"Processing {pdf_path}")
        try:
            counts = process_file(pdf_path, conn)
            for k, v in counts.items():
                totals[k] += v
            logger.info(f"  → {counts}")
        except Exception as e:
            logger.error(f"  ✗ Failed: {e}")
            totals["errors"] += 1

    print(f"\nPipeline A complete: {totals}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Write `tests/pipeline_a/test_integration.py`**

```python
"""Integration tests — run against real PDFs. Slow."""
import sqlite3
import pytest
from pathlib import Path
from pipelines.pipeline_a.run import process_file
from pipelines.shared.db import create_schema


@pytest.mark.slow
def test_budget_2025_produces_expenditure_rows():
    conn = sqlite3.connect(":memory:")
    create_schema(conn)
    counts = process_file(Path("resources/budgets/2025.pdf"), conn)
    assert counts["expenditures"] > 0, "Expected at least one expenditure row from FY2025 budget"


@pytest.mark.slow
def test_budget_2025_general_fund_amount_is_plausible():
    """The General Fund total should be in the tens-of-millions range."""
    conn = sqlite3.connect(":memory:")
    create_schema(conn)
    process_file(Path("resources/budgets/2025.pdf"), conn)
    row = conn.execute(
        "SELECT amount FROM expenditures WHERE department='Police' AND amount_type='adopted' LIMIT 1"
    ).fetchone()
    assert row is not None, "Expected a Police department row"
    assert row[0] > 1_000_000, f"Police budget too low: {row[0]}"


@pytest.mark.slow
def test_rerunning_is_idempotent():
    conn = sqlite3.connect(":memory:")
    create_schema(conn)
    process_file(Path("resources/budgets/2025.pdf"), conn)
    count_first = conn.execute("SELECT COUNT(*) FROM expenditures").fetchone()[0]
    process_file(Path("resources/budgets/2025.pdf"), conn)
    count_second = conn.execute("SELECT COUNT(*) FROM expenditures").fetchone()[0]
    assert count_first == count_second
```

- [ ] **Step 3: Run unit tests only (fast)**

```bash
uv run pytest tests/ -m "not slow" -v
```

Expected: all non-slow tests PASS

- [ ] **Step 4: Run integration test against real PDF**

```bash
uv run pytest tests/pipeline_a/test_integration.py -v -m slow
```

Expected: tests PASS. If `test_budget_2025_general_fund_amount_is_plausible` fails because the `Police` column name doesn't match, inspect the actual column names:

```bash
uv run python -c "
import pdfplumber, pandas as pd
with pdfplumber.open('resources/budgets/2025.pdf') as pdf:
    t = pdf.pages[67].extract_table()
    print(pd.DataFrame(t[1:], columns=t[0]).to_string())
"
```

Update `_SKIP_NAMES` or department name detection in `normalizer.py` as needed.

- [ ] **Step 5: Run full pipeline on all PDFs**

```bash
uv run pipelines/pipeline_a/run.py
```

Review the log output. Note any files that produce 0 rows — these need their page ranges corrected in `page_map.py`.

- [ ] **Step 6: Commit**

```bash
git add pipelines/pipeline_a/run.py tests/pipeline_a/test_integration.py
git commit -m "feat: pipeline_a run.py entry point and integration tests"
```
