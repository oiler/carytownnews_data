import pandas as pd
import pytest
from unittest.mock import patch, MagicMock, call
from pipelines.pipeline_a.extractor import extract_text_pages, extract_table_pages


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_page(text: str | None = None, tables: list[list[list]] | None = None):
    page = MagicMock()
    page.extract_text.return_value = text
    page.extract_tables.return_value = tables or []
    return page


def _make_pdf(pages: list):
    """Wrap a list of mock pages in a mock pdfplumber PDF context manager."""
    mock_pdf = MagicMock()
    mock_pdf.__enter__ = lambda s: s
    mock_pdf.__exit__ = MagicMock(return_value=False)
    mock_pdf.pages = pages
    return mock_pdf


# ── extract_text_pages ────────────────────────────────────────────────────────

def test_extract_text_returns_text_for_pages():
    pages = [
        _make_page("intro text"),               # p1
        _make_page("BUDGET OVERVIEW data"),     # p2
        _make_page("more budget data"),          # p3
    ]
    mock_pdf = _make_pdf(pages)

    with patch("pdfplumber.open", return_value=mock_pdf):
        result = extract_text_pages("fake.pdf", [2, 3])

    assert 2 in result
    assert 3 in result
    assert result[2] == "BUDGET OVERVIEW data"
    assert result[3] == "more budget data"


def test_extract_text_skips_pages_with_no_text():
    pages = [_make_page(None), _make_page("text here")]
    mock_pdf = _make_pdf(pages)

    with patch("pdfplumber.open", return_value=mock_pdf):
        result = extract_text_pages("fake.pdf", [1, 2])

    assert 1 not in result  # None text skipped
    assert 2 in result


def test_extract_text_skips_empty_string_pages():
    pages = [_make_page(""), _make_page("real text")]
    mock_pdf = _make_pdf(pages)

    with patch("pdfplumber.open", return_value=mock_pdf):
        result = extract_text_pages("fake.pdf", [1, 2])

    assert 1 not in result  # empty string skipped
    assert 2 in result


def test_extract_text_handles_page_error_gracefully():
    page_ok = _make_page("good text")
    page_err = MagicMock()
    page_err.extract_text.side_effect = Exception("PDF error")
    pages = [page_err, page_ok]
    mock_pdf = _make_pdf(pages)

    with patch("pdfplumber.open", return_value=mock_pdf):
        result = extract_text_pages("fake.pdf", [1, 2])

    assert 1 not in result  # error page skipped
    assert 2 in result      # good page still extracted


def test_extract_text_returns_only_requested_pages():
    pages = [_make_page(f"page {i}") for i in range(1, 6)]
    mock_pdf = _make_pdf(pages)

    with patch("pdfplumber.open", return_value=mock_pdf):
        result = extract_text_pages("fake.pdf", [2, 4])

    assert set(result.keys()) == {2, 4}


# ── extract_table_pages ───────────────────────────────────────────────────────

def test_extract_tables_returns_dataframes():
    raw_tables = [
        [["Fund", "YTD Actual", "Annual Budget"],
         ["General Fund", "130,000,000", "200,000,000"]],
    ]
    pages = [_make_page(tables=raw_tables)]
    mock_pdf = _make_pdf(pages)

    with patch("pdfplumber.open", return_value=mock_pdf):
        result = extract_table_pages("fake.pdf", [1])

    assert 1 in result
    assert isinstance(result[1], list)
    assert len(result[1]) == 1
    df = result[1][0]
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["Fund", "YTD Actual", "Annual Budget"]


def test_extract_tables_handles_multiple_tables_per_page():
    raw_tables = [
        [["Col1", "Val1"], ["a", "1"]],
        [["Col2", "Val2"], ["b", "2"]],
    ]
    pages = [_make_page(tables=raw_tables)]
    mock_pdf = _make_pdf(pages)

    with patch("pdfplumber.open", return_value=mock_pdf):
        result = extract_table_pages("fake.pdf", [1])

    assert len(result[1]) == 2


def test_extract_tables_skips_page_with_no_tables():
    pages = [_make_page(tables=[]), _make_page(tables=[[["A", "B"], ["1", "2"]]])]
    mock_pdf = _make_pdf(pages)

    with patch("pdfplumber.open", return_value=mock_pdf):
        result = extract_table_pages("fake.pdf", [1, 2])

    assert 1 not in result
    assert 2 in result


def test_extract_tables_skips_single_column_tables():
    """Single-column tables are likely headers/footers, not data."""
    raw_tables = [
        [["OnlyColumn"], ["row1"], ["row2"]],  # single column — skip
        [["Col1", "Col2"], ["a", "b"]],         # two columns — keep
    ]
    pages = [_make_page(tables=raw_tables)]
    mock_pdf = _make_pdf(pages)

    with patch("pdfplumber.open", return_value=mock_pdf):
        result = extract_table_pages("fake.pdf", [1])

    assert 1 in result
    assert len(result[1]) == 1  # only the 2-column table kept
    assert list(result[1][0].columns) == ["Col1", "Col2"]


def test_extract_tables_handles_page_error_gracefully():
    page_err = MagicMock()
    page_err.extract_tables.side_effect = Exception("parse error")
    pages = [page_err, _make_page(tables=[[["A", "B"], ["1", "2"]]])]
    mock_pdf = _make_pdf(pages)

    with patch("pdfplumber.open", return_value=mock_pdf):
        result = extract_table_pages("fake.pdf", [1, 2])

    assert 1 not in result
    assert 2 in result
