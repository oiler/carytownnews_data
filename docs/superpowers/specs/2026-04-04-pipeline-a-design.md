# Pipeline A — PDF Table Extraction (pdfplumber/camelot)
Date: 2026-04-04

## Overview

A Python pipeline that extracts financial data from Town of Cary PDF documents using
pdfplumber and camelot. Targets only the pages known to contain financial tables,
normalizes the output to a shared schema, and loads into SQLite. Runs manually when
new PDFs are added.

## Goals

- Extract expenditures, revenues, and fund summaries from budget PDFs (2021–2026),
  Annual Comprehensive Financial Reports (2019–2025), and quarterly reports (2021-Q1
  through 2026-Q2).
- Produce rows matching the shared schema so results can be compared against Pipeline B.
- Fail gracefully: log errors per page/document without crashing the full run.

## Data Schema

Shared with Pipeline B. Three tables in `data/cary.db`:

```sql
expenditures (id, pipeline, source_file, doc_type, fiscal_year, quarter, fund,
              department, division, amount_type, amount, extracted_at)

revenues (id, pipeline, source_file, doc_type, fiscal_year, quarter, fund,
          source, amount_type, amount, extracted_at)

fund_summaries (id, pipeline, source_file, doc_type, fiscal_year, quarter, fund,
                total_revenues, total_expenditures, transfers_in, transfers_out,
                beginning_balance, ending_balance, extracted_at)
```

`pipeline` is always `'A'`. `amount_type` values: `adopted`, `actual`, `prior_year_actual`.
`quarter` is NULL for annual documents.

## Architecture

```
pipelines/
  pipeline_a/
    page_map.py       # Hardcoded page ranges per doc type and fiscal year
    extractor.py      # pdfplumber primary, camelot fallback
    normalizer.py     # Per-doc-type normalization functions
    load.py           # Upsert to SQLite
    run.py            # Entry point
  shared/
    db.py             # Schema creation, upsert helpers
    schema.py         # Dataclasses for Expenditure, Revenue, FundSummary rows
tests/
  pipeline_a/
    fixtures/         # Sample raw pdfplumber output (captured from real PDFs)
    test_page_map.py
    test_extractor.py
    test_normalizer.py
    test_load.py
    test_integration.py
```

## Components

### `page_map.py`
Returns the page ranges to extract for a given file. Page numbers are 1-indexed and
may vary slightly between fiscal years — the map must be verified per year.

```python
# Returns: {section_name: [start_page, end_page], ...}
get_page_map(doc_type: str, fiscal_year: int) -> dict[str, list[int]]
```

Initial page ranges (FY2025 budget as baseline):
- `expenditure_summary`: [68, 68]
- `revenue_summary`: [92, 93]
- `fund_summary`: [65, 65]
- `dept_profiles`: [116, 200]

ACFR and quarterly ranges to be determined by inspection of each document.

### `extractor.py`
Opens a PDF, extracts tables from specified pages. Returns raw DataFrames, one per page.
Uses pdfplumber first; falls back to camelot (lattice mode) if pdfplumber returns an
empty or single-column result.

```python
extract_tables(pdf_path: str, pages: list[int]) -> list[tuple[int, pd.DataFrame]]
# Returns: [(page_number, dataframe), ...]
```

### `normalizer.py`
One function per document type. Takes raw DataFrames and returns typed schema rows.
Responsibilities: strip `$`/`,`/`%`, rename columns to schema names, map department
name variants to canonical names, infer `amount_type` from column headers.

```python
normalize_budget(tables: list[tuple[int, pd.DataFrame]], source_file: str, fiscal_year: int) -> NormalizedResult
normalize_acfr(...)
normalize_quarterly(...)
```

`NormalizedResult` contains lists of `Expenditure`, `Revenue`, and `FundSummary` dataclasses.

### `load.py`
Upserts rows to SQLite. Natural key: `(pipeline, source_file, doc_type, fiscal_year,
quarter, fund, department, division, amount_type)` for expenditures. Re-running is safe.

### `run.py`
Entry point. Discovers all PDFs in `resources/`, infers `doc_type` and `fiscal_year`
from directory and filename, calls each component in sequence, logs failures per file
without stopping the run. Prints a summary at the end.

```bash
uv run pipelines/pipeline_a/run.py
uv run pipelines/pipeline_a/run.py --file resources/budgets/2025.pdf  # single file
```

## Testing Strategy — TDD (Red/Green)

Tests are written before implementation. Each component has its own test file.

### `test_page_map.py`
- Given `doc_type='budget'`, `fiscal_year=2025` → returns expected page ranges
- Given unknown fiscal year → raises ValueError with helpful message

### `test_extractor.py`
- Given a real PDF path and known page → returns a non-empty DataFrame
- Given a page with no table → returns empty DataFrame, does not raise
- pdfplumber fallback: given a page that pdfplumber returns empty for → camelot is called

### `test_normalizer.py` (fixture-driven)
Fixtures are captured raw pdfplumber output saved as JSON/CSV in `tests/pipeline_a/fixtures/`.
- `normalize_budget()` with budget fixture → correct Expenditure rows with right amounts/types
- Dollar amounts with commas and `$` signs are parsed to floats correctly
- Rows with missing department name are dropped (logged as warnings)
- `normalize_quarterly()` with quarterly fixture → correct quarter/fiscal_year inferred

### `test_load.py`
Uses an in-memory SQLite database.
- Inserting a row and re-inserting the same row → table has exactly one row (idempotent)
- Inserting rows with different `amount_type` → both rows present

### `test_integration.py`
Runs the full pipeline against a single real PDF (`resources/budgets/2025.pdf`).
Marked `@pytest.mark.slow` — excluded from default test run.
- After run, SQLite contains expenditure rows for FY2025
- Total General Fund expenditures match the figure on budget page 68 (within rounding)

## Error Handling

- Page extraction failures: log the page number and exception, continue to next page
- Normalization failures: log the source file and row, skip the row
- Missing PDF file: log and skip, continue with remaining files
- No tables found on expected page: log as a warning (may indicate page number drift)

## Dependencies

```
pdfplumber
camelot-py[cv]
pandas
```

## Out of Scope

- Automatic detection of new PDFs
- Pipeline B logic
- Any web interface or visualization
