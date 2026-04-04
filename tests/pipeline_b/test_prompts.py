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
