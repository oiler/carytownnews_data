# Town of Cary Financial Data

Extracts financial data from Town of Cary, NC public documents (adopted budgets, ACFRs, quarterly reports) and loads it into a local SQLite database for analysis.

## Overview

Two independent extraction pipelines write to the same SQLite schema, allowing comparison of results:

- **Pipeline A** — text-based extraction using pdfplumber. Fast, free, works well on budget and quarterly reports.
- **Pipeline B** — vision-based extraction using Claude Haiku (Anthropic API). Handles complex layouts; costs ~$0.01–0.05 per document.

Both pipelines produce rows in `data/cary.db` with a `pipeline` column (`'A'` or `'B'`) so results can be compared.

## Documents

PDFs are stored in `resources/`:

```
resources/
  budgets/           2021–2025 adopted budgets
  financial-reports/ 2019–2023 ACFRs (Annual Comprehensive Financial Reports)
  quarterly-reports/ 2021-Q1 through 2024-Q3 quarterly highlights
```

## Setup

**Prerequisites:** Python 3.11+, [uv](https://docs.astral.sh/uv/getting-started/installation/)

```bash
# Install dependencies
uv pip install -e ".[dev]"

# Pipeline B only: set up your Anthropic API key
cp .env.example .env
# then edit .env and replace the placeholder with your actual key
```

Pipeline A works without any API key. Pipeline B requires an `ANTHROPIC_API_KEY` — get one at console.anthropic.com.

## Running Pipeline A

Text extraction — no API key required.

```bash
# All documents
uv run pipelines/pipeline_a/run.py

# Single file
uv run pipelines/pipeline_a/run.py --file resources/budgets/2025.pdf
```

## Running Pipeline B

Vision extraction via Claude Haiku — requires `ANTHROPIC_API_KEY` in `.env`.

```bash
source .env

# All documents (~$6–20 total)
uv run pipelines/pipeline_b/run.py

# Single file
uv run pipelines/pipeline_b/run.py --file resources/budgets/2025.pdf

# Dry run — renders pages, prints byte counts, no API calls
uv run pipelines/pipeline_b/run.py --dry-run
```

Cost is printed after each file and as a total at the end.

## Querying the Data

```bash
sqlite3 data/cary.db

# Compare pipelines on the 2025 budget
SELECT pipeline, COUNT(*) as rows, SUM(amount) as total
FROM expenditures
WHERE source_file LIKE '%2025%'
GROUP BY pipeline;

# All General Fund expenditures from Pipeline A
SELECT fiscal_year, department, amount_type, amount
FROM expenditures
WHERE pipeline='A' AND fund='General'
ORDER BY fiscal_year, department;
```

## Running Tests

```bash
# Fast tests only (no API calls, no real PDFs)
uv run pytest

# Include slow tests (real PDFs, real API calls)
source .env && uv run pytest -m slow
```

## Schema

```sql
expenditures   (pipeline, source_file, doc_type, fiscal_year, quarter, fund,
                department, division, amount_type, amount, extracted_at)

revenues       (pipeline, source_file, doc_type, fiscal_year, quarter, fund,
                source, amount_type, amount, extracted_at)

fund_summaries (pipeline, source_file, doc_type, fiscal_year, quarter, fund,
                total_revenues, total_expenditures, transfers_in, transfers_out,
                beginning_balance, ending_balance, extracted_at)
```

`amount_type` values: `adopted`, `actual`, `prior_year_actual`, `recommended`, `estimated`.
