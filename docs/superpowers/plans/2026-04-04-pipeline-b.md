# Pipeline B — Claude Vision Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract financial data from Town of Cary PDF documents by rendering pages to images and sending them to Claude (claude-haiku-4-5) with structured prompts, loading results into the same SQLite schema as Pipeline A.

**Architecture:** `renderer.py` converts PDF pages to PNG bytes using pymupdf. `claude_extractor.py` sends images to the Anthropic API and returns parsed JSON. `normalizer.py` converts JSON to typed schema rows. `run.py` orchestrates everything and logs running API cost. Reuses `pipelines/shared/` from Pipeline A.

**Tech Stack:** Python 3.12+, anthropic SDK, pymupdf (fitz), sqlite3, pytest, unittest.mock, uv

**Prerequisites:** Pipeline A's shared infrastructure (`pipelines/shared/schema.py`, `pipelines/shared/db.py`) must exist. `ANTHROPIC_API_KEY` must be set in `.env`.

---

## File Map

```
pipelines/
  pipeline_b/
    __init__.py
    page_map.py          # Same page ranges as Pipeline A — import or copy
    renderer.py          # PDF page → PNG bytes via pymupdf
    prompts.py           # Prompt templates per doc_type
    claude_extractor.py  # Anthropic API call, retry, cost tracking
    normalizer.py        # Claude JSON → schema dataclasses
    run.py               # Entry point with --dry-run and cost summary
tests/
  pipeline_b/
    __init__.py
    fixtures/
      claude_budget_response.json     # Saved real API response for unit tests
      claude_quarterly_response.json
    test_renderer.py
    test_prompts.py
    test_claude_extractor.py
    test_normalizer.py
    test_integration.py              # @pytest.mark.slow, makes real API calls
.env                                 # ANTHROPIC_API_KEY=sk-ant-...
```

---

## Task 1: Setup and `renderer.py`

**Files:**
- Create: `pipelines/pipeline_b/__init__.py`
- Create: `tests/pipeline_b/__init__.py`
- Create: `tests/pipeline_b/fixtures/` (directory)
- Create: `pipelines/pipeline_b/renderer.py`
- Test: `tests/pipeline_b/test_renderer.py`

- [ ] **Step 1: Create `.env` and scaffold directories**

```bash
touch pipelines/pipeline_b/__init__.py tests/pipeline_b/__init__.py
mkdir -p tests/pipeline_b/fixtures
```

Create `.env` in the project root:

```bash
ANTHROPIC_API_KEY=sk-ant-your-key-here
```

Add to `.gitignore`:

```
.env
data/
logs/
```

- [ ] **Step 2: Write failing tests for `renderer.py`**

Create `tests/pipeline_b/test_renderer.py`:

```python
import pytest
from pathlib import Path
from pipelines.pipeline_b.renderer import render_page

BUDGET_PDF = Path("resources/budgets/2025.pdf")
PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


@pytest.mark.slow
def test_render_page_returns_bytes():
    result = render_page(str(BUDGET_PDF), 68)
    assert isinstance(result, bytes)
    assert len(result) > 0


@pytest.mark.slow
def test_render_page_returns_valid_png():
    result = render_page(str(BUDGET_PDF), 68)
    assert result[:8] == PNG_MAGIC, "Expected PNG file signature"


@pytest.mark.slow
def test_render_page_different_pages_produce_different_bytes():
    page_68 = render_page(str(BUDGET_PDF), 68)
    page_92 = render_page(str(BUDGET_PDF), 92)
    assert page_68 != page_92


def test_render_page_invalid_pdf_raises():
    with pytest.raises(Exception):
        render_page("nonexistent.pdf", 1)


@pytest.mark.slow
def test_render_page_invalid_page_number_raises():
    with pytest.raises(ValueError, match="page"):
        render_page(str(BUDGET_PDF), 99999)
```

- [ ] **Step 3: Run tests — verify they fail**

```bash
uv run pytest tests/pipeline_b/test_renderer.py -v -m slow
```

Expected: `ModuleNotFoundError`

- [ ] **Step 4: Write `pipelines/pipeline_b/renderer.py`**

```python
import fitz  # pymupdf


def render_page(pdf_path: str, page_number: int, dpi: int = 150) -> bytes:
    """
    Render a single PDF page (1-indexed) to PNG bytes at the given DPI.
    Raises ValueError if page_number exceeds the document length.
    Raises fitz.FileNotFoundError if pdf_path does not exist.
    """
    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    if page_number < 1 or page_number > total_pages:
        doc.close()
        raise ValueError(
            f"page {page_number} out of range for {pdf_path} ({total_pages} pages)"
        )
    page = doc[page_number - 1]
    zoom = dpi / 72  # pymupdf default DPI is 72
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat)
    png_bytes = pix.tobytes("png")
    doc.close()
    return png_bytes
```

- [ ] **Step 5: Run tests — verify they pass**

```bash
uv run pytest tests/pipeline_b/test_renderer.py -v -m slow
```

Expected: all 5 tests PASS

- [ ] **Step 6: Commit**

```bash
git add pipelines/pipeline_b/ tests/pipeline_b/ .env .gitignore
git commit -m "feat: pipeline_b scaffold and renderer with TDD"
```

---

## Task 2: `page_map.py` and `prompts.py`

**Files:**
- Create: `pipelines/pipeline_b/page_map.py`
- Create: `pipelines/pipeline_b/prompts.py`
- Test: `tests/pipeline_b/test_prompts.py`

- [ ] **Step 1: Create `pipelines/pipeline_b/page_map.py`**

Pipeline B uses the same page ranges as Pipeline A. Rather than duplicating, import directly:

```python
# pipelines/pipeline_b/page_map.py
from pipelines.pipeline_a.page_map import get_pages  # noqa: F401
```

If Pipeline A is not installed or page ranges diverge in the future, copy the dict from `pipeline_a/page_map.py` here instead.

- [ ] **Step 2: Write failing tests for `prompts.py`**

Create `tests/pipeline_b/test_prompts.py`:

```python
import json
import pytest
from pipelines.pipeline_b.prompts import get_prompt


def test_budget_prompt_mentions_budget():
    prompt = get_prompt("budget")
    assert "budget" in prompt.lower()


def test_budget_prompt_contains_json_keys():
    prompt = get_prompt("budget")
    assert "expenditures" in prompt
    assert "revenues" in prompt
    assert "fund_summaries" in prompt


def test_quarterly_prompt_differs_from_budget():
    budget = get_prompt("budget")
    quarterly = get_prompt("quarterly")
    assert budget != quarterly


def test_acfr_prompt_differs_from_budget():
    budget = get_prompt("budget")
    acfr = get_prompt("acfr")
    assert budget != acfr


def test_unknown_doc_type_raises():
    with pytest.raises(ValueError, match="Unknown doc_type"):
        get_prompt("unknown")


def test_prompt_specifies_null_for_missing_fields():
    prompt = get_prompt("budget")
    assert "null" in prompt.lower()


def test_prompt_requests_amounts_as_numbers():
    prompt = get_prompt("budget")
    # Prompt should instruct Claude to return numbers, not strings with $ or ,
    assert "$" not in prompt or "no $" in prompt.lower() or "without" in prompt.lower()
```

- [ ] **Step 3: Run tests — verify they fail**

```bash
uv run pytest tests/pipeline_b/test_prompts.py -v
```

Expected: `ModuleNotFoundError`

- [ ] **Step 4: Write `pipelines/pipeline_b/prompts.py`**

```python
_SHARED_INSTRUCTIONS = """
Return ONLY valid JSON. No markdown, no explanation, no code fences.
Return null for any field you cannot find — do not hallucinate values.
Return all dollar amounts as plain numbers (e.g. 45230000, not "$45,230,000").
"""

_BUDGET_PROMPT = """
This is a page from a Town of Cary, North Carolina adopted operating budget document.
Extract all financial data visible on this page.

{shared}

Return this exact JSON structure:
{{
  "expenditures": [
    {{
      "fund": "General",
      "department": "Police",
      "division": null,
      "amount_type": "adopted",
      "amount": 45230000
    }}
  ],
  "revenues": [
    {{
      "fund": "General",
      "source": "Property Tax",
      "amount_type": "adopted",
      "amount": 120000000
    }}
  ],
  "fund_summaries": [
    {{
      "fund": "General",
      "total_revenues": 200000000,
      "total_expenditures": 195000000,
      "transfers_in": 5000000,
      "transfers_out": 10000000,
      "beginning_balance": 50000000,
      "ending_balance": 50000000
    }}
  ]
}}

amount_type must be one of: "adopted", "actual", "prior_year_actual", "recommended".
If a page shows multiple years, include one entry per year per department with the correct amount_type.
If a section is not present on this page, return an empty array for that key.
""".format(shared=_SHARED_INSTRUCTIONS)

_QUARTERLY_PROMPT = """
This is a page from a Town of Cary, North Carolina Council Quarterly Report.
Extract the financial highlights data visible on this page.

{shared}

Return this exact JSON structure:
{{
  "expenditures": [],
  "revenues": [],
  "fund_summaries": [
    {{
      "fund": "General Fund",
      "total_revenues": null,
      "total_expenditures": 130000000,
      "transfers_in": null,
      "transfers_out": null,
      "beginning_balance": null,
      "ending_balance": null
    }}
  ]
}}

If a section is not present on this page, return an empty array for that key.
""".format(shared=_SHARED_INSTRUCTIONS)

_ACFR_PROMPT = """
This is a page from the Town of Cary, North Carolina Annual Comprehensive Financial Report (ACFR).
Extract all financial statement data visible on this page.

{shared}

Return this exact JSON structure:
{{
  "expenditures": [
    {{
      "fund": "General",
      "department": "Public Safety",
      "division": null,
      "amount_type": "actual",
      "amount": 45230000
    }}
  ],
  "revenues": [
    {{
      "fund": "General",
      "source": "Property Tax",
      "amount_type": "actual",
      "amount": 120000000
    }}
  ],
  "fund_summaries": [
    {{
      "fund": "General",
      "total_revenues": 200000000,
      "total_expenditures": 195000000,
      "transfers_in": 5000000,
      "transfers_out": 10000000,
      "beginning_balance": 50000000,
      "ending_balance": 50000000
    }}
  ]
}}

If a section is not present on this page, return an empty array for that key.
""".format(shared=_SHARED_INSTRUCTIONS)

_PROMPTS = {
    "budget": _BUDGET_PROMPT,
    "quarterly": _QUARTERLY_PROMPT,
    "acfr": _ACFR_PROMPT,
}


def get_prompt(doc_type: str) -> str:
    if doc_type not in _PROMPTS:
        raise ValueError(
            f"Unknown doc_type: {doc_type!r}. Expected one of: {list(_PROMPTS)}"
        )
    return _PROMPTS[doc_type]
```

- [ ] **Step 5: Run tests — verify they pass**

```bash
uv run pytest tests/pipeline_b/test_prompts.py -v
```

Expected: all 7 tests PASS

- [ ] **Step 6: Commit**

```bash
git add pipelines/pipeline_b/page_map.py pipelines/pipeline_b/prompts.py tests/pipeline_b/test_prompts.py
git commit -m "feat: pipeline_b page_map and prompts with TDD"
```

---

## Task 3: `claude_extractor.py`

**Files:**
- Create: `pipelines/pipeline_b/claude_extractor.py`
- Test: `tests/pipeline_b/test_claude_extractor.py`

- [ ] **Step 1: Write failing tests**

Create `tests/pipeline_b/test_claude_extractor.py`:

```python
import json
import pytest
from dataclasses import dataclass
from unittest.mock import MagicMock, patch
from anthropic import APIStatusError
import httpx

from pipelines.pipeline_b.claude_extractor import extract_page, ExtractResult

VALID_RESPONSE_JSON = json.dumps({
    "expenditures": [
        {"fund": "General", "department": "Police", "division": None,
         "amount_type": "adopted", "amount": 45230000}
    ],
    "revenues": [],
    "fund_summaries": []
})

FAKE_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100


def _make_api_response(content_text: str, input_tokens: int = 1500, output_tokens: int = 300):
    """Build a mock Anthropic message response."""
    msg = MagicMock()
    msg.content = [MagicMock(text=content_text)]
    msg.usage.input_tokens = input_tokens
    msg.usage.output_tokens = output_tokens
    return msg


def _make_client(response):
    client = MagicMock()
    client.messages.create.return_value = response
    return client


def test_extract_page_returns_extract_result():
    client = _make_client(_make_api_response(VALID_RESPONSE_JSON))
    result = extract_page(FAKE_PNG, "budget prompt", client)
    assert isinstance(result, ExtractResult)


def test_extract_page_parses_json_correctly():
    client = _make_client(_make_api_response(VALID_RESPONSE_JSON))
    result = extract_page(FAKE_PNG, "budget prompt", client)
    assert len(result.data["expenditures"]) == 1
    assert result.data["expenditures"][0]["department"] == "Police"
    assert result.data["expenditures"][0]["amount"] == 45230000


def test_extract_page_returns_token_counts():
    client = _make_client(_make_api_response(VALID_RESPONSE_JSON, input_tokens=2000, output_tokens=400))
    result = extract_page(FAKE_PNG, "budget prompt", client)
    assert result.input_tokens == 2000
    assert result.output_tokens == 400


def test_extract_page_returns_empty_data_on_invalid_json():
    client = _make_client(_make_api_response("this is not json"))
    result = extract_page(FAKE_PNG, "budget prompt", client)
    assert result.data == {"expenditures": [], "revenues": [], "fund_summaries": []}


def test_extract_page_retries_on_api_status_error():
    good_response = _make_api_response(VALID_RESPONSE_JSON)
    error = APIStatusError(
        "rate limit",
        response=MagicMock(spec=httpx.Response, status_code=429),
        body={}
    )
    client = MagicMock()
    client.messages.create.side_effect = [error, good_response]

    with patch("time.sleep"):  # don't actually sleep in tests
        result = extract_page(FAKE_PNG, "budget prompt", client)

    assert client.messages.create.call_count == 2
    assert result.data["expenditures"][0]["department"] == "Police"


def test_extract_page_raises_after_three_failures():
    error = APIStatusError(
        "server error",
        response=MagicMock(spec=httpx.Response, status_code=500),
        body={}
    )
    client = MagicMock()
    client.messages.create.side_effect = error

    with patch("time.sleep"):
        with pytest.raises(APIStatusError):
            extract_page(FAKE_PNG, "budget prompt", client)

    assert client.messages.create.call_count == 3
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
uv run pytest tests/pipeline_b/test_claude_extractor.py -v
```

Expected: `ModuleNotFoundError`

- [ ] **Step 3: Write `pipelines/pipeline_b/claude_extractor.py`**

```python
import base64
import json
import logging
import time
from dataclasses import dataclass, field

from anthropic import Anthropic, APIStatusError

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
_EMPTY_DATA = {"expenditures": [], "revenues": [], "fund_summaries": []}
_MAX_RETRIES = 3
_RETRY_BASE_SECONDS = 2


@dataclass
class ExtractResult:
    data: dict
    input_tokens: int
    output_tokens: int


def extract_page(image_bytes: bytes, prompt: str, client: Anthropic) -> ExtractResult:
    """
    Send a PNG image to Claude and return structured financial data.
    Retries up to 3 times on APIStatusError with exponential backoff.
    Returns empty data (not raises) on invalid JSON response.
    """
    b64 = base64.standard_b64encode(image_bytes).decode()

    for attempt in range(_MAX_RETRIES):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=2048,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": "image/png",
                                    "data": b64,
                                },
                            },
                            {"type": "text", "text": prompt},
                        ],
                    }
                ],
            )
            text = response.content[0].text
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                logger.warning(f"Claude returned invalid JSON: {text[:200]!r}")
                data = _EMPTY_DATA.copy()

            return ExtractResult(
                data=data,
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
            )

        except APIStatusError as e:
            if attempt < _MAX_RETRIES - 1:
                wait = _RETRY_BASE_SECONDS * (2 ** attempt)
                logger.warning(f"API error (attempt {attempt + 1}/{_MAX_RETRIES}): {e}. Retrying in {wait}s.")
                time.sleep(wait)
            else:
                raise
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
uv run pytest tests/pipeline_b/test_claude_extractor.py -v
```

Expected: all 7 tests PASS

- [ ] **Step 5: Save a real API response as a fixture**

This fixture lets future tests run without API calls:

```bash
uv run python -c "
import json, os
from anthropic import Anthropic
from pipelines.pipeline_b.renderer import render_page
from pipelines.pipeline_b.prompts import get_prompt
from pipelines.pipeline_b.claude_extractor import extract_page

client = Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])
png = render_page('resources/budgets/2025.pdf', 68)
prompt = get_prompt('budget')
result = extract_page(png, prompt, client)
with open('tests/pipeline_b/fixtures/claude_budget_response.json', 'w') as f:
    json.dump(result.data, f, indent=2)
print('Saved. Tokens:', result.input_tokens, '+', result.output_tokens)
"
```

Repeat for a quarterly report:

```bash
uv run python -c "
import json, os
from anthropic import Anthropic
from pipelines.pipeline_b.renderer import render_page
from pipelines.pipeline_b.prompts import get_prompt
from pipelines.pipeline_b.claude_extractor import extract_page

client = Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])
png = render_page('resources/quarterly-reports/2024-q3.pdf', 7)
prompt = get_prompt('quarterly')
result = extract_page(png, prompt, client)
with open('tests/pipeline_b/fixtures/claude_quarterly_response.json', 'w') as f:
    json.dump(result.data, f, indent=2)
print('Saved. Tokens:', result.input_tokens, '+', result.output_tokens)
"
```

- [ ] **Step 6: Commit**

```bash
git add pipelines/pipeline_b/claude_extractor.py tests/pipeline_b/test_claude_extractor.py tests/pipeline_b/fixtures/
git commit -m "feat: pipeline_b claude_extractor with retry logic and TDD"
```

---

## Task 4: `normalizer.py`

**Files:**
- Create: `pipelines/pipeline_b/normalizer.py`
- Test: `tests/pipeline_b/test_normalizer.py`

- [ ] **Step 1: Write failing tests**

Create `tests/pipeline_b/test_normalizer.py`:

```python
import json
import pytest
from pathlib import Path
from pipelines.pipeline_b.normalizer import normalize
from pipelines.shared.schema import NormalizedResult

SOURCE = "resources/budgets/2025.pdf"
FIXTURE_DIR = Path("tests/pipeline_b/fixtures")


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text())


def test_normalize_returns_normalized_result():
    raw = {
        "expenditures": [
            {"fund": "General", "department": "Police", "division": None,
             "amount_type": "adopted", "amount": 45230000}
        ],
        "revenues": [],
        "fund_summaries": [],
    }
    result = normalize(raw, SOURCE, "budget", 2025, None)
    assert isinstance(result, NormalizedResult)


def test_normalize_expenditure_fields():
    raw = {
        "expenditures": [
            {"fund": "General", "department": "Police", "division": None,
             "amount_type": "adopted", "amount": 45230000}
        ],
        "revenues": [],
        "fund_summaries": [],
    }
    result = normalize(raw, SOURCE, "budget", 2025, None)
    assert len(result.expenditures) == 1
    exp = result.expenditures[0]
    assert exp.pipeline == "B"
    assert exp.source_file == SOURCE
    assert exp.doc_type == "budget"
    assert exp.fiscal_year == 2025
    assert exp.quarter is None
    assert exp.fund == "General"
    assert exp.department == "Police"
    assert exp.division is None
    assert exp.amount_type == "adopted"
    assert exp.amount == 45_230_000.0


def test_normalize_revenue_fields():
    raw = {
        "expenditures": [],
        "revenues": [
            {"fund": "General", "source": "Property Tax",
             "amount_type": "adopted", "amount": 120000000}
        ],
        "fund_summaries": [],
    }
    result = normalize(raw, SOURCE, "budget", 2025, None)
    assert len(result.revenues) == 1
    rev = result.revenues[0]
    assert rev.pipeline == "B"
    assert rev.source == "Property Tax"
    assert rev.amount == 120_000_000.0


def test_normalize_fund_summary_fields():
    raw = {
        "expenditures": [],
        "revenues": [],
        "fund_summaries": [
            {"fund": "General", "total_revenues": 200000000,
             "total_expenditures": 195000000, "transfers_in": 5000000,
             "transfers_out": 10000000, "beginning_balance": 50000000,
             "ending_balance": 50000000}
        ],
    }
    result = normalize(raw, SOURCE, "budget", 2025, None)
    assert len(result.fund_summaries) == 1
    fs = result.fund_summaries[0]
    assert fs.fund == "General"
    assert fs.total_revenues == 200_000_000.0
    assert fs.ending_balance == 50_000_000.0


def test_normalize_sets_quarterly_quarter():
    raw = {"expenditures": [], "revenues": [], "fund_summaries": [
        {"fund": "General", "total_revenues": None, "total_expenditures": 130000000,
         "transfers_in": None, "transfers_out": None, "beginning_balance": None, "ending_balance": None}
    ]}
    result = normalize(raw, "resources/quarterly-reports/2024-q3.pdf", "quarterly", 2024, 3)
    assert result.fund_summaries[0].quarter == 3


def test_normalize_drops_row_missing_required_field():
    raw = {
        "expenditures": [
            {"fund": "General", "department": None, "division": None,
             "amount_type": "adopted", "amount": 45230000},  # no department
            {"fund": "General", "department": "Police", "division": None,
             "amount_type": "adopted", "amount": 45230000},
        ],
        "revenues": [],
        "fund_summaries": [],
    }
    result = normalize(raw, SOURCE, "budget", 2025, None)
    assert len(result.expenditures) == 1
    assert result.expenditures[0].department == "Police"


def test_normalize_amount_none_drops_row():
    raw = {
        "expenditures": [
            {"fund": "General", "department": "Police", "division": None,
             "amount_type": "adopted", "amount": None},
        ],
        "revenues": [],
        "fund_summaries": [],
    }
    result = normalize(raw, SOURCE, "budget", 2025, None)
    assert len(result.expenditures) == 0


@pytest.mark.slow
def test_normalize_budget_fixture():
    """Uses the real API fixture saved in Task 3."""
    raw = load_fixture("claude_budget_response.json")
    result = normalize(raw, SOURCE, "budget", 2025, None)
    total_rows = len(result.expenditures) + len(result.revenues) + len(result.fund_summaries)
    assert total_rows > 0, "Expected at least one row from the real budget fixture"
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
uv run pytest tests/pipeline_b/test_normalizer.py -v -m "not slow"
```

Expected: `ModuleNotFoundError`

- [ ] **Step 3: Write `pipelines/pipeline_b/normalizer.py`**

```python
import logging
from datetime import datetime, timezone

from pipelines.shared.schema import Expenditure, FundSummary, NormalizedResult, Revenue

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def normalize(
    raw: dict,
    source_file: str,
    doc_type: str,
    fiscal_year: int,
    quarter: int | None,
) -> NormalizedResult:
    """
    Convert Claude's JSON response to typed schema rows.
    Drops rows with missing required fields or null amounts.
    """
    now = _now()
    expenditures: list[Expenditure] = []
    revenues: list[Revenue] = []
    fund_summaries: list[FundSummary] = []

    for item in raw.get("expenditures", []) or []:
        dept = item.get("department")
        amount = item.get("amount")
        if not dept or amount is None:
            logger.debug(f"Dropping expenditure row with missing dept/amount: {item}")
            continue
        try:
            expenditures.append(Expenditure(
                pipeline="B",
                source_file=source_file,
                doc_type=doc_type,
                fiscal_year=fiscal_year,
                quarter=quarter,
                fund=item.get("fund") or "General",
                department=dept,
                division=item.get("division"),
                amount_type=item.get("amount_type") or "adopted",
                amount=float(amount),
                extracted_at=now,
            ))
        except (TypeError, ValueError) as e:
            logger.warning(f"Skipping malformed expenditure row {item}: {e}")

    for item in raw.get("revenues", []) or []:
        source = item.get("source")
        amount = item.get("amount")
        if not source or amount is None:
            logger.debug(f"Dropping revenue row with missing source/amount: {item}")
            continue
        try:
            revenues.append(Revenue(
                pipeline="B",
                source_file=source_file,
                doc_type=doc_type,
                fiscal_year=fiscal_year,
                quarter=quarter,
                fund=item.get("fund") or "General",
                source=source,
                amount_type=item.get("amount_type") or "adopted",
                amount=float(amount),
                extracted_at=now,
            ))
        except (TypeError, ValueError) as e:
            logger.warning(f"Skipping malformed revenue row {item}: {e}")

    for item in raw.get("fund_summaries", []) or []:
        fund = item.get("fund")
        if not fund:
            continue
        def _f(key: str) -> float | None:
            v = item.get(key)
            return float(v) if v is not None else None
        fund_summaries.append(FundSummary(
            pipeline="B",
            source_file=source_file,
            doc_type=doc_type,
            fiscal_year=fiscal_year,
            quarter=quarter,
            fund=fund,
            total_revenues=_f("total_revenues"),
            total_expenditures=_f("total_expenditures"),
            transfers_in=_f("transfers_in"),
            transfers_out=_f("transfers_out"),
            beginning_balance=_f("beginning_balance"),
            ending_balance=_f("ending_balance"),
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
uv run pytest tests/pipeline_b/test_normalizer.py -v -m "not slow"
```

Expected: all non-slow tests PASS

- [ ] **Step 5: Run fixture test (uses real API response from Task 3)**

```bash
uv run pytest tests/pipeline_b/test_normalizer.py::test_normalize_budget_fixture -v -m slow
```

Expected: PASS. If it fails because the fixture has 0 rows, inspect the fixture file and adjust the prompt in `prompts.py`.

- [ ] **Step 6: Commit**

```bash
git add pipelines/pipeline_b/normalizer.py tests/pipeline_b/test_normalizer.py
git commit -m "feat: pipeline_b normalizer with TDD"
```

---

## Task 5: `run.py` and integration test

**Files:**
- Create: `pipelines/pipeline_b/run.py`
- Create: `logs/` (directory, gitignored)
- Test: `tests/pipeline_b/test_integration.py`

- [ ] **Step 1: Write `pipelines/pipeline_b/run.py`**

```python
"""
Pipeline B entry point.

Usage:
    uv run pipelines/pipeline_b/run.py
    uv run pipelines/pipeline_b/run.py --file resources/budgets/2025.pdf
    uv run pipelines/pipeline_b/run.py --dry-run
"""
import argparse
import json
import logging
import os
from pathlib import Path

from anthropic import Anthropic

from pipelines.pipeline_b.claude_extractor import extract_page
from pipelines.pipeline_b.normalizer import normalize
from pipelines.pipeline_b.page_map import get_pages
from pipelines.pipeline_b.prompts import get_prompt
from pipelines.pipeline_b.renderer import render_page
from pipelines.shared.db import (
    create_schema,
    get_connection,
    upsert_expenditures,
    upsert_fund_summaries,
    upsert_revenues,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Haiku pricing per 1M tokens (as of April 2026)
_INPUT_COST_PER_M = 0.80
_OUTPUT_COST_PER_M = 4.00

RESOURCE_DIRS = {
    "budgets": "budget",
    "financial-reports": "acfr",
    "quarterly-reports": "quarterly",
}


def parse_filename(path: Path) -> tuple[str, int, int | None]:
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


def _compute_cost(input_tokens: int, output_tokens: int) -> float:
    return (input_tokens / 1_000_000) * _INPUT_COST_PER_M + \
           (output_tokens / 1_000_000) * _OUTPUT_COST_PER_M


def process_file(pdf_path: Path, client: Anthropic, conn, dry_run: bool = False) -> dict:
    doc_type, fiscal_year, quarter = parse_filename(pdf_path)
    page_map = get_pages(doc_type, fiscal_year)
    prompt = get_prompt(doc_type)

    total_input_tokens = 0
    total_output_tokens = 0
    counts = {"expenditures": 0, "revenues": 0, "fund_summaries": 0}
    log_dir = Path("logs")

    for section, (start, end) in page_map.items():
        for page_num in range(start, end + 1):
            try:
                png = render_page(str(pdf_path), page_num)
                if dry_run:
                    logger.info(f"  [dry-run] would send p{page_num} ({len(png)} bytes)")
                    continue

                result = extract_page(png, prompt, client)
                total_input_tokens += result.input_tokens
                total_output_tokens += result.output_tokens

                # Log raw response for debugging
                log_dir.mkdir(exist_ok=True)
                log_path = log_dir / f"pipeline_b_{pdf_path.stem}_p{page_num}.json"
                log_path.write_text(json.dumps(result.data, indent=2))

                normalized = normalize(result.data, str(pdf_path), doc_type, fiscal_year, quarter)
                counts["expenditures"] += upsert_expenditures(conn, normalized.expenditures)
                counts["revenues"] += upsert_revenues(conn, normalized.revenues)
                counts["fund_summaries"] += upsert_fund_summaries(conn, normalized.fund_summaries)

            except Exception as e:
                logger.error(f"  ✗ Failed p{page_num} of {pdf_path}: {e}")

    cost = _compute_cost(total_input_tokens, total_output_tokens)
    return {**counts, "input_tokens": total_input_tokens, "output_tokens": total_output_tokens, "cost_usd": cost}


def discover_pdfs(resource_root: Path = Path("resources")) -> list[Path]:
    return sorted(resource_root.rglob("*.pdf"))


def main(args: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Pipeline B: Claude vision extraction")
    parser.add_argument("--file", help="Process a single PDF file")
    parser.add_argument("--dry-run", action="store_true",
                        help="Render pages and estimate cost without making API calls")
    parsed = parser.parse_args(args)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key and not parsed.dry_run:
        raise SystemExit("ANTHROPIC_API_KEY not set. Add it to .env and run: source .env")

    client = Anthropic(api_key=api_key or "dry-run")
    conn = get_connection()
    create_schema(conn)

    pdfs = [Path(parsed.file)] if parsed.file else discover_pdfs()
    total_cost = 0.0
    total_errors = 0

    for pdf_path in pdfs:
        logger.info(f"Processing {pdf_path}")
        try:
            result = process_file(pdf_path, client, conn, dry_run=parsed.dry_run)
            total_cost += result.get("cost_usd", 0)
            logger.info(f"  → rows: {result}  cumulative cost: ${total_cost:.4f}")
        except Exception as e:
            logger.error(f"  ✗ Failed {pdf_path}: {e}")
            total_errors += 1

    print(f"\nPipeline B complete. Total cost: ${total_cost:.4f}. Errors: {total_errors}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Write `tests/pipeline_b/test_integration.py`**

```python
"""Integration tests — make real API calls. Requires ANTHROPIC_API_KEY."""
import os
import sqlite3
import pytest
from pathlib import Path
from anthropic import Anthropic
from pipelines.pipeline_b.run import process_file
from pipelines.shared.db import create_schema


@pytest.mark.slow
def test_pipeline_b_budget_page_produces_rows():
    """Process a single known page (p68 = expenditure summary) and expect rows."""
    from pipelines.pipeline_b.run import parse_filename
    from pipelines.pipeline_b.claude_extractor import extract_page
    from pipelines.pipeline_b.renderer import render_page
    from pipelines.pipeline_b.prompts import get_prompt
    from pipelines.pipeline_b.normalizer import normalize
    from pipelines.shared.db import upsert_expenditures, upsert_revenues, upsert_fund_summaries

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        pytest.skip("ANTHROPIC_API_KEY not set")

    client = Anthropic(api_key=api_key)
    conn = sqlite3.connect(":memory:")
    create_schema(conn)

    png = render_page("resources/budgets/2025.pdf", 68)
    prompt = get_prompt("budget")
    result = extract_page(png, prompt, client)

    assert result.input_tokens > 0
    assert result.output_tokens > 0

    normalized = normalize(result.data, "resources/budgets/2025.pdf", "budget", 2025, None)
    total_rows = len(normalized.expenditures) + len(normalized.revenues) + len(normalized.fund_summaries)
    assert total_rows > 0, f"Expected rows from Claude. Raw response: {result.data}"


@pytest.mark.slow
def test_pipeline_b_cost_is_tracked():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        pytest.skip("ANTHROPIC_API_KEY not set")

    client = Anthropic(api_key=api_key)
    conn = sqlite3.connect(":memory:")
    create_schema(conn)

    result = process_file(Path("resources/budgets/2025.pdf"), client, conn)
    assert result["cost_usd"] > 0
    assert result["input_tokens"] > 0
```

- [ ] **Step 3: Run all unit tests (no API calls)**

```bash
uv run pytest tests/ -m "not slow" -v
```

Expected: all PASS

- [ ] **Step 4: Run dry-run to verify page rendering**

```bash
uv run pipelines/pipeline_b/run.py --dry-run --file resources/budgets/2025.pdf
```

Expected: logs each page being rendered, no API calls, no errors.

- [ ] **Step 5: Run integration test (costs ~$0.02)**

```bash
source .env && uv run pytest tests/pipeline_b/test_integration.py -v -m slow
```

Expected: PASS. If Claude returns empty data, inspect `logs/pipeline_b_2025_p68.json` and refine the prompt in `prompts.py`.

- [ ] **Step 6: Run both pipelines on a single file and compare**

```bash
source .env
uv run pipelines/pipeline_a/run.py --file resources/budgets/2025.pdf
uv run pipelines/pipeline_b/run.py --file resources/budgets/2025.pdf
```

Query the DB to compare:

```bash
sqlite3 data/cary.db "
SELECT pipeline, COUNT(*) as rows, SUM(amount) as total
FROM expenditures
WHERE source_file='resources/budgets/2025.pdf'
GROUP BY pipeline;
"
```

- [ ] **Step 7: Commit**

```bash
git add pipelines/pipeline_b/run.py tests/pipeline_b/test_integration.py
git commit -m "feat: pipeline_b run.py with dry-run, cost tracking, and integration tests"
```
