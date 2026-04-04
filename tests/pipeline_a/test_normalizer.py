import pytest
from datetime import datetime, timezone
from pipelines.pipeline_a.normalizer import (
    _parse_amount,
    normalize_budget,
    normalize_quarterly,
    normalize_acfr,
)
from pipelines.shared.schema import NormalizedResult

SOURCE_BUDGET = "resources/budgets/2025.pdf"
SOURCE_QUARTERLY = "resources/quarterly-reports/2024-q3.pdf"
SOURCE_ACFR = "resources/financial-reports/2024.pdf"

# ── _parse_amount ─────────────────────────────────────────────────────────────

def test_parse_amount_plain_integer():
    assert _parse_amount("45230000") == 45_230_000.0

def test_parse_amount_with_commas():
    assert _parse_amount("45,230,000") == 45_230_000.0

def test_parse_amount_with_dollar_sign():
    assert _parse_amount("$45,230,000") == 45_230_000.0

def test_parse_amount_accounting_negative():
    assert _parse_amount("(1,000)") == -1_000.0

def test_parse_amount_dash_returns_none():
    assert _parse_amount("-") is None

def test_parse_amount_empty_returns_none():
    assert _parse_amount("") is None

def test_parse_amount_none_input_returns_none():
    assert _parse_amount(None) is None

def test_parse_amount_with_whitespace():
    assert _parse_amount("  $1,234  ") == 1_234.0

def test_parse_amount_decimal():
    assert _parse_amount("1,234.56") == 1_234.56

# ── normalize_budget ──────────────────────────────────────────────────────────

BUDGET_OVERVIEW_TEXT = """BUDGET OVERVIEW - ALL FUNDS
REVENUES
Actual Actual Estimated Adopted
FY 2022 FY 2023 FY2024 FY2025
Property Taxes 115,234,000 125,400,000 137,000,000 149,600,000
Sales Tax 29,454,000 32,100,000 35,500,000 38,200,000
Total Revenues 144,688,000 157,500,000 172,500,000 187,800,000
EXPENDITURES
General Government 15,234,000 16,800,000 17,500,000 18,900,000
Public Safety 84,232,000 89,400,000 95,000,000 102,300,000
Total Expenditures 99,466,000 106,200,000 112,500,000 121,200,000
"""

DEPT_PROFILE_TEXT = """DEPARTMENT PROFILE - POLICE
FY 2023 FY 2024 FY 2025
Actual Adopted Adopted
Personnel 28,400,000 30,100,000 32,500,000
Operations 12,300,000 13,200,000 14,100,000
Total 40,700,000 43,300,000 46,600,000
"""

def test_normalize_budget_returns_normalized_result():
    result = normalize_budget({74: BUDGET_OVERVIEW_TEXT}, SOURCE_BUDGET, 2025)
    assert isinstance(result, NormalizedResult)

def test_normalize_budget_extracts_revenue_rows():
    result = normalize_budget({74: BUDGET_OVERVIEW_TEXT}, SOURCE_BUDGET, 2025)
    sources = {r.source for r in result.revenues}
    assert "Property Taxes" in sources
    assert "Sales Tax" in sources

def test_normalize_budget_skips_total_rows():
    result = normalize_budget({74: BUDGET_OVERVIEW_TEXT}, SOURCE_BUDGET, 2025)
    sources = {r.source for r in result.revenues}
    assert "Total Revenues" not in sources

def test_normalize_budget_adopted_amount_correct():
    result = normalize_budget({74: BUDGET_OVERVIEW_TEXT}, SOURCE_BUDGET, 2025)
    prop_tax_adopted = next(
        r.amount for r in result.revenues
        if r.source == "Property Taxes" and r.amount_type == "adopted"
    )
    assert prop_tax_adopted == 149_600_000.0

def test_normalize_budget_extracts_expenditure_rows():
    result = normalize_budget({74: BUDGET_OVERVIEW_TEXT}, SOURCE_BUDGET, 2025)
    depts = {e.department for e in result.expenditures}
    assert "General Government" in depts
    assert "Public Safety" in depts

def test_normalize_budget_skips_expenditure_total_rows():
    result = normalize_budget({74: BUDGET_OVERVIEW_TEXT}, SOURCE_BUDGET, 2025)
    depts = {e.department for e in result.expenditures}
    assert "Total Expenditures" not in depts

def test_normalize_budget_dept_profile_extracts_department_name():
    result = normalize_budget({75: DEPT_PROFILE_TEXT}, SOURCE_BUDGET, 2025)
    depts = {e.department for e in result.expenditures}
    assert "POLICE" in depts or "Police" in depts  # accept either casing

def test_normalize_budget_pipeline_is_A():
    result = normalize_budget({74: BUDGET_OVERVIEW_TEXT}, SOURCE_BUDGET, 2025)
    all_rows = result.expenditures + result.revenues
    assert all(r.pipeline == "A" for r in all_rows)

def test_normalize_budget_fiscal_year_correct():
    result = normalize_budget({74: BUDGET_OVERVIEW_TEXT}, SOURCE_BUDGET, 2025)
    all_rows = result.expenditures + result.revenues
    assert all(r.fiscal_year == 2025 for r in all_rows)

def test_normalize_budget_quarter_is_none():
    result = normalize_budget({74: BUDGET_OVERVIEW_TEXT}, SOURCE_BUDGET, 2025)
    all_rows = result.expenditures + result.revenues
    assert all(r.quarter is None for r in all_rows)

# ── normalize_quarterly ───────────────────────────────────────────────────────

QUARTERLY_TEXT = """FINANCIAL HIGHLIGHTS FY 2024 Q3
General Fund Summary
FY 2024 YTD Actual FY 2024 YTD Budget
Revenues 270,100,000 183,700,000
Expenditures 195,400,000 175,300,000
"""

def test_normalize_quarterly_returns_normalized_result():
    result = normalize_quarterly({7: QUARTERLY_TEXT}, SOURCE_QUARTERLY, 2024, 3)
    assert isinstance(result, NormalizedResult)

def test_normalize_quarterly_extracts_fund_summary():
    result = normalize_quarterly({7: QUARTERLY_TEXT}, SOURCE_QUARTERLY, 2024, 3)
    assert len(result.fund_summaries) > 0

def test_normalize_quarterly_sets_quarter():
    result = normalize_quarterly({7: QUARTERLY_TEXT}, SOURCE_QUARTERLY, 2024, 3)
    assert all(fs.quarter == 3 for fs in result.fund_summaries)

def test_normalize_quarterly_sets_fiscal_year():
    result = normalize_quarterly({7: QUARTERLY_TEXT}, SOURCE_QUARTERLY, 2024, 3)
    assert all(fs.fiscal_year == 2024 for fs in result.fund_summaries)

def test_normalize_quarterly_pipeline_is_A():
    result = normalize_quarterly({7: QUARTERLY_TEXT}, SOURCE_QUARTERLY, 2024, 3)
    assert all(fs.pipeline == "A" for fs in result.fund_summaries)

# ── normalize_acfr ────────────────────────────────────────────────────────────

ACFR_TEXT = """Statement of Net Position
Governmental Activities Business-type Activities Total
Assets
Current assets:
Cash and investments 245,000,000 85,000,000 330,000,000
Total assets 412,000,000 210,000,000 622,000,000
Net Position
Unrestricted 45,000,000 22,000,000 67,000,000
Total net position 195,000,000 88,000,000 283,000,000
"""

def test_normalize_acfr_returns_normalized_result():
    result = normalize_acfr({50: ACFR_TEXT}, SOURCE_ACFR, 2024)
    assert isinstance(result, NormalizedResult)
    # ACFR returns empty rows — Pipeline B handles ACFR via Claude Vision
    assert result.expenditures == []
    assert result.revenues == []
    assert result.fund_summaries == []

def test_normalize_acfr_returns_empty_for_now():
    """Placeholder: ACFR normalizer intentionally returns empty (Pipeline B handles ACFR)."""
    result = normalize_acfr({50: ACFR_TEXT}, SOURCE_ACFR, 2024)
    assert isinstance(result, NormalizedResult)
