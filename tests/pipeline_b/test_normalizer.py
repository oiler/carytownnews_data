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
             "amount_type": "adopted", "amount": 45230000},  # no department → dropped
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
