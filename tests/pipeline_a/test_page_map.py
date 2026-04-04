import pytest
from pipelines.pipeline_a.page_map import get_anchors, SectionAnchor


def test_budget_returns_budget_overview_anchor():
    anchors = get_anchors("budget")
    keys = [a.section for a in anchors]
    assert "budget_overview" in keys


def test_budget_overview_anchor_has_keyword():
    anchors = get_anchors("budget")
    overview = next(a for a in anchors if a.section == "budget_overview")
    assert "BUDGET OVERVIEW" in overview.keyword


def test_budget_returns_dept_profiles_anchor():
    anchors = get_anchors("budget")
    keys = [a.section for a in anchors]
    assert "dept_profiles" in keys


def test_dept_profiles_anchor_collects_all():
    anchors = get_anchors("budget")
    dept = next(a for a in anchors if a.section == "dept_profiles")
    assert dept.collect_all is True


def test_quarterly_returns_financial_highlights_anchor():
    anchors = get_anchors("quarterly")
    keys = [a.section for a in anchors]
    assert "financial_highlights" in keys


def test_quarterly_pages_after_is_positive():
    anchors = get_anchors("quarterly")
    fh = next(a for a in anchors if a.section == "financial_highlights")
    assert fh.pages_after > 0


def test_acfr_returns_anchors():
    anchors = get_anchors("acfr")
    assert len(anchors) > 0


def test_unknown_doc_type_raises():
    with pytest.raises(ValueError, match="Unknown doc_type"):
        get_anchors("unknown")


def test_returns_list_of_section_anchors():
    anchors = get_anchors("budget")
    assert all(isinstance(a, SectionAnchor) for a in anchors)
