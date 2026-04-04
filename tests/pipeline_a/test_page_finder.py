import pytest
from unittest.mock import patch, MagicMock
from pipelines.pipeline_a.page_finder import find_section_pages
from pipelines.pipeline_a.page_map import SectionAnchor


def _make_mock_pdf(page_texts: list[str]):
    """Return a mock pdfplumber PDF with given page texts (1 per page)."""
    mock_pdf = MagicMock()
    mock_pdf.__enter__ = lambda s: s
    mock_pdf.__exit__ = MagicMock(return_value=False)
    pages = []
    for text in page_texts:
        p = MagicMock()
        p.extract_text.return_value = text
        pages.append(p)
    mock_pdf.pages = pages
    return mock_pdf


def test_find_section_pages_finds_keyword_on_correct_page():
    texts = ["intro text", "more intro", "BUDGET OVERVIEW section here with numbers", "other content"]
    mock_pdf = _make_mock_pdf(texts)
    anchor = SectionAnchor(section="budget_overview", keyword="BUDGET OVERVIEW", pages_after=1, collect_all=False)

    with patch("pdfplumber.open", return_value=mock_pdf):
        result = find_section_pages("fake.pdf", [anchor])

    assert "budget_overview" in result
    assert 3 in result["budget_overview"]  # page 3 (1-indexed)


def test_find_section_pages_includes_pages_after():
    texts = ["intro", "BUDGET OVERVIEW here", "next page data", "unrelated"]
    mock_pdf = _make_mock_pdf(texts)
    anchor = SectionAnchor(section="budget_overview", keyword="BUDGET OVERVIEW", pages_after=1, collect_all=False)

    with patch("pdfplumber.open", return_value=mock_pdf):
        result = find_section_pages("fake.pdf", [anchor])

    pages = result["budget_overview"]
    assert 2 in pages  # keyword found on page 2
    assert 3 in pages  # pages_after=1, so include next page too


def test_find_section_pages_collect_all_finds_multiple_pages():
    texts = [
        "intro",
        "DEPARTMENT PROFILE - Police",
        "DEPARTMENT PROFILE - Parks",
        "unrelated",
        "DEPARTMENT PROFILE - Public Works",
    ]
    mock_pdf = _make_mock_pdf(texts)
    anchor = SectionAnchor(section="dept_profiles", keyword="DEPARTMENT PROFILE", pages_after=0, collect_all=True)

    with patch("pdfplumber.open", return_value=mock_pdf):
        result = find_section_pages("fake.pdf", [anchor])

    pages = result["dept_profiles"]
    assert 2 in pages
    assert 3 in pages
    assert 5 in pages
    assert 4 not in pages  # "unrelated" page not included


def test_find_section_pages_returns_empty_list_when_keyword_not_found():
    texts = ["page one", "page two", "page three"]
    mock_pdf = _make_mock_pdf(texts)
    anchor = SectionAnchor(section="budget_overview", keyword="BUDGET OVERVIEW", pages_after=1, collect_all=False)

    with patch("pdfplumber.open", return_value=mock_pdf):
        result = find_section_pages("fake.pdf", [anchor])

    assert result["budget_overview"] == []


def test_find_section_pages_handles_page_with_no_text():
    texts = [None, "BUDGET OVERVIEW here", "data page"]
    mock_pdf = _make_mock_pdf(texts)
    anchor = SectionAnchor(section="budget_overview", keyword="BUDGET OVERVIEW", pages_after=1, collect_all=False)

    with patch("pdfplumber.open", return_value=mock_pdf):
        result = find_section_pages("fake.pdf", [anchor])

    assert 2 in result["budget_overview"]


def test_find_section_pages_multiple_anchors():
    texts = [
        "intro",
        "BUDGET OVERVIEW revenue and expenditure",
        "budget detail",
        "DEPARTMENT PROFILE - Police",
        "DEPARTMENT PROFILE - Parks",
    ]
    mock_pdf = _make_mock_pdf(texts)
    anchors = [
        SectionAnchor(section="budget_overview", keyword="BUDGET OVERVIEW", pages_after=1, collect_all=False),
        SectionAnchor(section="dept_profiles", keyword="DEPARTMENT PROFILE", pages_after=0, collect_all=True),
    ]

    with patch("pdfplumber.open", return_value=mock_pdf):
        result = find_section_pages("fake.pdf", anchors)

    assert 2 in result["budget_overview"]
    assert 3 in result["budget_overview"]
    assert 4 in result["dept_profiles"]
    assert 5 in result["dept_profiles"]
