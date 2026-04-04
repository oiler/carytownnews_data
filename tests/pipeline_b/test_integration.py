"""Integration tests — make real API calls. Requires ANTHROPIC_API_KEY."""
import os
import sqlite3
import pytest
from pathlib import Path
from anthropic import Anthropic
from pipelines.pipeline_b.claude_extractor import extract_page
from pipelines.pipeline_b.normalizer import normalize
from pipelines.pipeline_b.prompts import get_prompt
from pipelines.pipeline_b.renderer import render_page
from pipelines.pipeline_b.run import parse_filename, process_file
from pipelines.shared.db import create_schema, upsert_expenditures, upsert_revenues, upsert_fund_summaries


@pytest.mark.slow
def test_pipeline_b_budget_page_produces_rows():
    """Process a single known page (p68 = budget overview) and expect rows."""
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
