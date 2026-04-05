# Data Viewer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Datasette-based data viewer for `data/cary.db` with human-readable metadata and pre-written canned queries for common analysis tasks.

**Architecture:** Install `datasette` and `datasette-vega` as dev dependencies. Create `datasette.yaml` with table/column descriptions and five SQL canned queries. Update README with the launch command. No application code — the entire viewer is the YAML config.

**Tech Stack:** [datasette](https://datasette.io/) ≥ 0.65, datasette-vega ≥ 0.6, SQLite (`data/cary.db`)

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `pyproject.toml` | Modify | Add datasette and datasette-vega to `[dev]` optional dependencies |
| `datasette.yaml` | Create | Metadata (table/column descriptions) and canned queries |
| `README.md` | Modify | Add "Viewing the Data" section with launch command |

---

### Task 1: Add dependencies

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add datasette and datasette-vega to dev deps**

Open `pyproject.toml` and update the `[project.optional-dependencies]` section to:

```toml
[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-mock>=3.14",
    "datasette>=0.65",
    "datasette-vega>=0.6",
]
```

- [ ] **Step 2: Install the new dependencies**

```bash
uv pip install -e ".[dev]"
```

Expected: both `datasette` and `datasette-vega` appear in the install output with no errors.

- [ ] **Step 3: Verify datasette is available**

```bash
datasette --version
```

Expected: prints a version string like `datasette, version 0.65.x` (or 1.x if a newer version resolved).

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore: add datasette and datasette-vega dev dependencies"
```

---

### Task 2: Create datasette.yaml

**Files:**
- Create: `datasette.yaml`

- [ ] **Step 1: Create datasette.yaml with metadata and canned queries**

Create `datasette.yaml` in the project root with this exact content:

```yaml
title: Cary Civic Data
description: Town of Cary financial data — budgets, ACFRs, quarterly reports

databases:
  cary:
    tables:
      expenditures:
        description: "Departmental expenditure rows from budgets, ACFRs, and quarterly reports"
        columns:
          pipeline: "A = pdfplumber · B = Claude Vision"
          amount_type: "adopted, actual, prior_year_actual, recommended, or estimated"
          doc_type: "budget, acfr, or quarterly"
          quarter: "1–4 for quarterly reports; NULL for annual documents"
          extracted_at: "Timestamp when this row was written to the database"
      revenues:
        description: "Revenue rows from budgets, ACFRs, and quarterly reports"
        columns:
          pipeline: "A = pdfplumber · B = Claude Vision"
          amount_type: "adopted, actual, prior_year_actual, recommended, or estimated"
          doc_type: "budget, acfr, or quarterly"
          quarter: "1–4 for quarterly reports; NULL for annual documents"
          extracted_at: "Timestamp when this row was written to the database"
      fund_summaries:
        description: "Fund-level totals with revenues, expenditures, transfers, and balances"
        columns:
          pipeline: "A = pdfplumber · B = Claude Vision"
          doc_type: "budget, acfr, or quarterly"
          quarter: "1–4 for quarterly reports; NULL for annual documents"
          extracted_at: "Timestamp when this row was written to the database"
    queries:
      pipeline_comparison:
        title: "Pipeline Comparison — Expenditures"
        description: "Side-by-side Pipeline A vs B for the same document, year, fund, and department"
        sql: |
          SELECT
            a.doc_type,
            a.fiscal_year,
            a.source_file,
            a.fund,
            a.department,
            a.division,
            a.amount_type,
            a.amount                      AS pipeline_a,
            b.amount                      AS pipeline_b,
            ROUND(a.amount - b.amount, 2) AS difference
          FROM expenditures a
          JOIN expenditures b
            ON  a.doc_type    = b.doc_type
            AND a.fiscal_year = b.fiscal_year
            AND a.source_file = b.source_file
            AND a.fund        = b.fund
            AND a.department  = b.department
            AND COALESCE(a.division, '') = COALESCE(b.division, '')
            AND a.amount_type = b.amount_type
          WHERE a.pipeline = 'A'
            AND b.pipeline = 'B'
          ORDER BY a.doc_type, a.fiscal_year, a.fund, a.department
      pipeline_disagreements:
        title: "Pipeline Disagreements — Expenditures"
        description: "Rows where A and B extracted different amounts, ordered by discrepancy size"
        sql: |
          SELECT
            a.doc_type,
            a.fiscal_year,
            a.source_file,
            a.fund,
            a.department,
            a.division,
            a.amount_type,
            a.amount                             AS pipeline_a,
            b.amount                             AS pipeline_b,
            ROUND(ABS(a.amount - b.amount), 2)   AS discrepancy
          FROM expenditures a
          JOIN expenditures b
            ON  a.doc_type    = b.doc_type
            AND a.fiscal_year = b.fiscal_year
            AND a.source_file = b.source_file
            AND a.fund        = b.fund
            AND a.department  = b.department
            AND COALESCE(a.division, '') = COALESCE(b.division, '')
            AND a.amount_type = b.amount_type
          WHERE a.pipeline = 'A'
            AND b.pipeline = 'B'
            AND ABS(a.amount - b.amount) > 0.01
          ORDER BY discrepancy DESC
      year_over_year:
        title: "Year-Over-Year Expenditure Totals"
        description: "Total expenditures by fiscal year, pipeline, and amount type"
        sql: |
          SELECT
            fiscal_year,
            pipeline,
            amount_type,
            ROUND(SUM(amount), 2) AS total
          FROM expenditures
          GROUP BY fiscal_year, pipeline, amount_type
          ORDER BY fiscal_year, pipeline, amount_type
      fund_totals:
        title: "Fund Totals by Year"
        description: "Total expenditures by fund, fiscal year, pipeline, and amount type"
        sql: |
          SELECT
            fund,
            fiscal_year,
            pipeline,
            amount_type,
            ROUND(SUM(amount), 2) AS total
          FROM expenditures
          GROUP BY fund, fiscal_year, pipeline, amount_type
          ORDER BY fund, fiscal_year, pipeline, amount_type
      extraction_coverage:
        title: "Extraction Coverage"
        description: "Row counts per pipeline, document type, and fiscal year — shows what has been extracted and what is missing"
        sql: |
          SELECT
            pipeline,
            doc_type,
            fiscal_year,
            COUNT(*)          AS row_count,
            MIN(extracted_at) AS first_extracted,
            MAX(extracted_at) AS last_extracted
          FROM expenditures
          GROUP BY pipeline, doc_type, fiscal_year
          ORDER BY pipeline, doc_type, fiscal_year
```

- [ ] **Step 2: Launch datasette and smoke test**

```bash
datasette data/cary.db --metadata datasette.yaml
```

Open `http://127.0.0.1:8001` in a browser. Verify:
- Page title shows "Cary Civic Data"
- `expenditures`, `revenues`, and `fund_summaries` tables are listed
- Click into `expenditures` — column descriptions appear as tooltip text
- Click "Queries" or navigate to the `cary` database page — the 5 canned queries are listed
- Click "Extraction Coverage" — query runs and returns a result table (may be empty if no data has been ingested yet, but must not error)
- Click "Pipeline Comparison" — query runs without SQL error

Stop datasette with Ctrl-C when done.

> **Note on datasette 1.0+:** If your installed version is 1.0 or higher and `--metadata` is unrecognised, use `--config` instead: `datasette data/cary.db --config datasette.yaml`. The YAML content is compatible with both.

- [ ] **Step 3: Commit**

```bash
git add datasette.yaml
git commit -m "feat: add Datasette viewer with metadata and canned queries"
```

---

### Task 3: Update README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add "Viewing the Data" section to README**

Insert the following section into `README.md` immediately before the `## Documents` section:

```markdown
## Viewing the Data

```bash
datasette data/cary.db --metadata datasette.yaml
```

Opens at `http://127.0.0.1:8001`. The database page lists five pre-written queries:

- **Pipeline Comparison** — side-by-side A vs B amounts for the same doc/year/fund/department
- **Pipeline Disagreements** — rows where A and B differ, ordered by discrepancy size
- **Year-Over-Year Totals** — total expenditures by fiscal year and pipeline
- **Fund Totals** — expenditures aggregated by fund and year
- **Extraction Coverage** — row counts per pipeline/doc type/year to see what's been extracted

Any query result can be exported as CSV or JSON via the links below the table. Charts are available via the Vega plugin button.

```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add Datasette viewer launch instructions to README"
```
