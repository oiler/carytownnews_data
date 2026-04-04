"""
Integration tests for Pipeline A run.py.
Slow tests hit real PDFs and are marked @pytest.mark.slow.
Unit tests for parse_filename run without any PDFs.
"""
import sqlite3
import pytest
from pathlib import Path
from pipelines.pipeline_a.run import parse_filename, process_file, discover_pdfs
from pipelines.shared.db import create_schema


# ── parse_filename unit tests (fast, no PDFs) ─────────────────────────────────

def test_parse_filename_budget():
    doc_type, year, quarter = parse_filename(Path("resources/budgets/2025.pdf"))
    assert doc_type == "budget"
    assert year == 2025
    assert quarter is None


def test_parse_filename_acfr():
    doc_type, year, quarter = parse_filename(Path("resources/financial-reports/2024.pdf"))
    assert doc_type == "acfr"
    assert year == 2024
    assert quarter is None


def test_parse_filename_quarterly_with_quarter():
    doc_type, year, quarter = parse_filename(Path("resources/quarterly-reports/2024-q3.pdf"))
    assert doc_type == "quarterly"
    assert year == 2024
    assert quarter == 3


def test_parse_filename_quarterly_annual():
    doc_type, year, quarter = parse_filename(Path("resources/quarterly-reports/2024-annual.pdf"))
    assert doc_type == "quarterly"
    assert year == 2024
    assert quarter is None


def test_parse_filename_unknown_directory_raises():
    with pytest.raises(ValueError, match="Unknown resource directory"):
        parse_filename(Path("unknown/2025.pdf"))


def test_parse_filename_bad_quarterly_stem_raises():
    with pytest.raises(ValueError, match="Cannot parse quarterly filename"):
        parse_filename(Path("resources/quarterly-reports/badname.pdf"))


# ── integration tests (slow, hit real PDFs) ───────────────────────────────────

@pytest.mark.slow
def test_process_budget_2025_produces_rows():
    conn = sqlite3.connect(":memory:")
    create_schema(conn)
    counts = process_file(Path("resources/budgets/2025.pdf"), conn)
    assert counts["revenues"] > 0 or counts["expenditures"] > 0, \
        f"Expected rows from FY2025 budget, got: {counts}"


@pytest.mark.slow
def test_process_budget_revenue_amounts_are_plausible():
    """Property tax revenue for a town of ~180k people should be in the hundreds of millions."""
    conn = sqlite3.connect(":memory:")
    create_schema(conn)
    process_file(Path("resources/budgets/2025.pdf"), conn)
    row = conn.execute(
        "SELECT amount FROM revenues WHERE source LIKE '%Property%' AND amount_type='adopted' LIMIT 1"
    ).fetchone()
    assert row is not None, "Expected a property tax revenue row"
    assert 50_000_000 < row[0] < 500_000_000, f"Property tax out of plausible range: {row[0]:,.0f}"


@pytest.mark.slow
def test_process_file_is_idempotent():
    conn = sqlite3.connect(":memory:")
    create_schema(conn)
    process_file(Path("resources/budgets/2025.pdf"), conn)
    count_first = conn.execute("SELECT COUNT(*) FROM revenues").fetchone()[0]
    process_file(Path("resources/budgets/2025.pdf"), conn)
    count_second = conn.execute("SELECT COUNT(*) FROM revenues").fetchone()[0]
    assert count_first == count_second, "Re-running created duplicate rows"


@pytest.mark.slow
def test_process_quarterly_report():
    conn = sqlite3.connect(":memory:")
    create_schema(conn)
    counts = process_file(Path("resources/quarterly-reports/2024-q3.pdf"), conn)
    assert counts["fund_summaries"] > 0, f"Expected fund summaries from quarterly report, got: {counts}"


@pytest.mark.slow
def test_discover_pdfs_finds_all_resource_pdfs():
    pdfs = discover_pdfs()
    assert len(pdfs) > 0
    # Should find budgets, financial-reports, and quarterly-reports
    doc_types = {p.parent.name for p in pdfs}
    assert "budgets" in doc_types
    assert "financial-reports" in doc_types
    assert "quarterly-reports" in doc_types
