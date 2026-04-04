# Pipeline B — Claude Vision Extraction
Date: 2026-04-04

## Overview

A Python pipeline that extracts financial data from Town of Cary PDF documents by
rendering target pages to images and sending them to the Claude API (claude-haiku-4-5)
with structured prompts. Returns JSON matching the shared schema and loads into SQLite.
Runs manually when new PDFs are added.

## Goals

- Extract the same expenditures, revenues, and fund summaries as Pipeline A, using
  Claude's vision capability instead of text/table parsing.
- Handle all three document types (budgets, ACFRs, quarterly reports) with a single
  consistent approach regardless of layout complexity.
- Track API cost per run.
- Produce rows in the shared schema so results can be compared against Pipeline A.

## Data Schema

Shared with Pipeline A. Same three tables in `data/cary.db`:

```sql
expenditures (id, pipeline, source_file, doc_type, fiscal_year, quarter, fund,
              department, division, amount_type, amount, extracted_at)

revenues (id, pipeline, source_file, doc_type, fiscal_year, quarter, fund,
          source, amount_type, amount, extracted_at)

fund_summaries (id, pipeline, source_file, doc_type, fiscal_year, quarter, fund,
                total_revenues, total_expenditures, transfers_in, transfers_out,
                beginning_balance, ending_balance, extracted_at)
```

`pipeline` is always `'B'`. Same `amount_type` values as Pipeline A.

## Architecture

```
pipelines/
  pipeline_b/
    page_map.py       # Same page ranges as Pipeline A (shared or copied)
    renderer.py       # PDF page → PNG in memory via pymupdf
    prompts.py        # Prompt templates per doc type
    claude_extractor.py  # Anthropic API call, retry logic, cost tracking
    normalizer.py     # Type-cast Claude JSON → schema dataclasses
    load.py           # Upsert to SQLite (same as Pipeline A)
    run.py            # Entry point
  shared/
    db.py             # Schema creation, upsert helpers
    schema.py         # Dataclasses for Expenditure, Revenue, FundSummary rows
tests/
  pipeline_b/
    fixtures/         # Saved Claude API responses (JSON) for unit tests
    test_renderer.py
    test_prompts.py
    test_claude_extractor.py
    test_normalizer.py
    test_integration.py
```

## Components

### `page_map.py`
Identical logic to Pipeline A's `page_map.py`. May be imported from `shared/` if page
ranges are the same (to avoid duplication), or kept as a copy if they diverge.

### `renderer.py`
Renders a single PDF page to a PNG image in memory at 150dpi using pymupdf (fitz).
Returns raw bytes. Never writes to disk.

```python
render_page(pdf_path: str, page_number: int, dpi: int = 150) -> bytes
```

150dpi chosen as the balance between legibility for Claude and token cost. If extraction
quality is poor on any document, bump to 200dpi for that doc.

### `prompts.py`
One prompt template per document type. Each prompt:
- States the document context ("This is a page from a Town of Cary, NC adopted budget")
- Specifies the exact JSON structure to return
- Instructs Claude to return `null` for fields it cannot find (not to hallucinate)
- Asks for amounts as numbers only (no `$` or `,`)

```python
get_prompt(doc_type: str) -> str
```

Example structure requested in prompt:
```json
{
  "expenditures": [
    {"fund": "General", "department": "Police", "division": null,
     "amount_type": "adopted", "amount": 45230000}
  ],
  "revenues": [...],
  "fund_summaries": [...]
}
```

### `claude_extractor.py`
Sends a rendered page image to `claude-haiku-4-5` using the Anthropic SDK.
Returns parsed JSON and token counts.

```python
extract_page(image_bytes: bytes, prompt: str, client: Anthropic) -> ExtractResult
# ExtractResult: {data: dict, input_tokens: int, output_tokens: int}
```

Retry logic: up to 3 retries on `APIStatusError` (rate limit/server error) with
exponential backoff (2s, 4s, 8s). Raises after 3 failures.

Invalid JSON in response: logged as an extraction failure, returns empty data for
that page rather than crashing.

### `normalizer.py`
Lighter than Pipeline A's normalizer — Claude already parses the data. This module
just type-casts (ensures amounts are `float`, years are `int`), validates required
fields are present, and converts dicts to schema dataclasses.

```python
normalize(raw: dict, source_file: str, doc_type: str,
          fiscal_year: int, quarter: int | None) -> NormalizedResult
```

### `load.py`
Same upsert logic as Pipeline A. Shared module in `shared/db.py`.

### `run.py`
Entry point. Discovers PDFs, renders target pages, extracts, normalizes, loads.
Prints running cost total after each file (input tokens × $0.80/1M + output tokens × $4.00/1M).

```bash
uv run pipelines/pipeline_b/run.py
uv run pipelines/pipeline_b/run.py --file resources/budgets/2025.pdf
uv run pipelines/pipeline_b/run.py --dry-run  # renders pages, estimates cost, no API calls
```

`--dry-run` renders all target pages and prints estimated cost without making any API calls.

## Testing Strategy — TDD (Red/Green)

Tests written before implementation. Fixtures are saved real API responses (JSON files)
so unit tests never call the Anthropic API.

### `test_renderer.py`
- `render_page()` with a real PDF → returns bytes of non-zero length
- Output is valid PNG (check magic bytes: `\x89PNG`)
- Invalid page number → raises with clear message

### `test_prompts.py`
- `get_prompt('budget')` → contains the word "budget" and the JSON structure keys
- `get_prompt('acfr')` → different from budget prompt
- Unknown doc type → raises ValueError

### `test_claude_extractor.py`
Uses `unittest.mock` to patch the Anthropic client — never calls real API.
- Successful response with valid JSON → returns parsed ExtractResult with correct token counts
- Response with invalid JSON → returns empty data, does not raise
- APIStatusError on first call, success on second → returns result (retry works)
- APIStatusError on all 3 retries → raises

### `test_normalizer.py` (fixture-driven)
Fixtures are JSON files matching the structure Claude returns, saved from real runs.
- Valid budget page response → correct Expenditure/Revenue/FundSummary dataclasses
- Missing `amount` field in a row → row is dropped, others retained
- Amount as string "1,234,567" in Claude response → normalized to float 1234567.0

### `test_integration.py`
Marked `@pytest.mark.slow` — makes real API calls, costs ~$0.02.
- Runs Pipeline B on a single page of `resources/budgets/2025.pdf` (page 68)
- SQLite contains at least one expenditure row with a non-zero amount
- Logged token count matches Anthropic response metadata

## Error Handling

- API failures after retries: log the file + page, skip, continue
- Invalid JSON from Claude: log raw response to `logs/pipeline_b_failures.jsonl` for inspection
- Renderer failure: log and skip the page
- Missing PDF: log and skip, continue with remaining files

## Cost Tracking

`run.py` accumulates `input_tokens` and `output_tokens` across all API calls and prints
a cost summary at the end of each run using Haiku pricing ($0.80/$4.00 per 1M tokens).

## Dependencies

```
anthropic
pymupdf
```

## Out of Scope

- Automatic detection of new PDFs
- Pipeline A logic
- Any web interface or visualization
- Switching models mid-run (Haiku only; upgrade to Sonnet manually if quality is poor)
