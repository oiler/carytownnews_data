# Data Viewer — Datasette
Date: 2026-04-05

## Overview

A Datasette-based data viewer for `data/cary.db`. Configured with a `datasette.yaml`
file providing human-readable metadata and pre-written canned queries for the most
common analysis tasks. No application code — the entire viewer is the YAML config.

## Goals

- Browse and spot-check pipeline output (are rows being extracted correctly?)
- Query and compare Pipeline A vs B results for the same documents
- Export query results to CSV/JSON for further analysis
- Surface light visualizations (bar/line charts) without a custom app

## Audience

Personal use only — local development tool, not deployed or shared.

## New Files

```
datasette.yaml    # metadata, canned queries, plugin config
```

No new directories. No application code.

## Dependencies

Added to `[project.optional-dependencies]` dev group in `pyproject.toml`:

```
datasette>=0.65
datasette-vega>=0.6
```

Install with the existing `uv pip install -r requirements.txt` workflow.

## Launch

```bash
datasette data/cary.db --metadata datasette.yaml
```

Opens at `http://127.0.0.1:8001`. Datasette reads live from SQLite on each request —
just refresh the browser after a pipeline run to see updated data.

## datasette.yaml Contents

### Table & Column Metadata

Human-readable descriptions for key columns:

| Column | Description |
|---|---|
| `pipeline` | A = pdfplumber extraction, B = Claude Vision extraction |
| `amount_type` | `adopted` = approved budget, `actual` = real spending, `prior_year_actual` = previous year actual |
| `doc_type` | `budget`, `acfr`, or `quarterly` |
| `quarter` | NULL for annual documents |
| `extracted_at` | Timestamp of when this row was written to the DB |

### Canned Queries

| Query | Purpose |
|---|---|
| `pipeline_comparison` | Side-by-side A vs B for the same doc/year/fund/department |
| `pipeline_disagreements` | Rows where A and B extracted different amounts for the same natural key |
| `year_over_year` | Total expenditures and revenues by fiscal year |
| `fund_totals` | Aggregated totals by fund and fiscal year |
| `extraction_coverage` | Row counts per pipeline per doc_type per fiscal_year — shows what's extracted and what's missing |

### Plugin Config

`datasette-vega` enabled so any query result can be charted (bar, line, scatter) with
a few clicks — no code required.

## Out of Scope

- Authentication or access control
- Deployment (local only)
- Custom CSS or branding
- Writing back to the DB
- Open data portal files (CSV etc.) — can be added later by passing additional paths to
  the `datasette` command if needed
