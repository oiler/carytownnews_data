import sqlite3
from datetime import datetime, timezone
from pipelines.shared.schema import Expenditure, Revenue, FundSummary, NormalizedResult
from pipelines.shared.db import create_schema, upsert_expenditures, upsert_revenues, upsert_fund_summaries

NOW = datetime(2026, 4, 4, 12, 0, 0, tzinfo=timezone.utc)


def make_conn():
    conn = sqlite3.connect(":memory:")
    create_schema(conn)
    return conn


def make_expenditure(**kwargs) -> Expenditure:
    defaults = dict(
        pipeline="A", source_file="resources/budgets/2025.pdf",
        doc_type="budget", fiscal_year=2025, quarter=None,
        fund="General", department="Police", division=None,
        amount_type="adopted", amount=45_230_000.0, extracted_at=NOW,
    )
    return Expenditure(**{**defaults, **kwargs})


def test_create_schema_creates_tables():
    conn = make_conn()
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    table_names = {row[0] for row in tables}
    assert "expenditures" in table_names
    assert "revenues" in table_names
    assert "fund_summaries" in table_names


def test_upsert_expenditure_inserts_row():
    conn = make_conn()
    rows = [make_expenditure()]
    upsert_expenditures(conn, rows)
    count = conn.execute("SELECT COUNT(*) FROM expenditures").fetchone()[0]
    assert count == 1


def test_upsert_expenditure_is_idempotent():
    conn = make_conn()
    row = make_expenditure()
    upsert_expenditures(conn, [row])
    upsert_expenditures(conn, [row])
    count = conn.execute("SELECT COUNT(*) FROM expenditures").fetchone()[0]
    assert count == 1


def test_upsert_expenditure_updates_amount_on_conflict():
    conn = make_conn()
    upsert_expenditures(conn, [make_expenditure(amount=1_000.0)])
    upsert_expenditures(conn, [make_expenditure(amount=2_000.0)])
    amount = conn.execute("SELECT amount FROM expenditures").fetchone()[0]
    assert amount == 2_000.0


def test_upsert_different_amount_types_are_separate_rows():
    conn = make_conn()
    upsert_expenditures(conn, [
        make_expenditure(amount_type="adopted"),
        make_expenditure(amount_type="actual"),
    ])
    count = conn.execute("SELECT COUNT(*) FROM expenditures").fetchone()[0]
    assert count == 2


def test_upsert_revenues():
    conn = make_conn()
    revenue = Revenue(
        pipeline="A", source_file="resources/budgets/2025.pdf",
        doc_type="budget", fiscal_year=2025, quarter=None,
        fund="General", source="Property Tax",
        amount_type="adopted", amount=120_000_000.0, extracted_at=NOW,
    )
    upsert_revenues(conn, [revenue])
    count = conn.execute("SELECT COUNT(*) FROM revenues").fetchone()[0]
    assert count == 1


def test_upsert_revenues_is_idempotent():
    conn = make_conn()
    revenue = Revenue(
        pipeline="A", source_file="resources/budgets/2025.pdf",
        doc_type="budget", fiscal_year=2025, quarter=None,
        fund="General", source="Property Tax",
        amount_type="adopted", amount=120_000_000.0, extracted_at=NOW,
    )
    upsert_revenues(conn, [revenue])
    upsert_revenues(conn, [revenue])
    count = conn.execute("SELECT COUNT(*) FROM revenues").fetchone()[0]
    assert count == 1


def test_upsert_fund_summary():
    conn = make_conn()
    summary = FundSummary(
        pipeline="A", source_file="resources/budgets/2025.pdf",
        doc_type="budget", fiscal_year=2025, quarter=None,
        fund="General", total_revenues=200_000_000.0,
        total_expenditures=195_000_000.0, transfers_in=5_000_000.0,
        transfers_out=10_000_000.0, beginning_balance=50_000_000.0,
        ending_balance=50_000_000.0, extracted_at=NOW,
    )
    upsert_fund_summaries(conn, [summary])
    count = conn.execute("SELECT COUNT(*) FROM fund_summaries").fetchone()[0]
    assert count == 1


def test_upsert_fund_summary_is_idempotent():
    conn = make_conn()
    summary = FundSummary(
        pipeline="A", source_file="resources/budgets/2025.pdf",
        doc_type="budget", fiscal_year=2025, quarter=None,
        fund="General", total_revenues=200_000_000.0,
        total_expenditures=195_000_000.0, transfers_in=5_000_000.0,
        transfers_out=10_000_000.0, beginning_balance=50_000_000.0,
        ending_balance=50_000_000.0, extracted_at=NOW,
    )
    upsert_fund_summaries(conn, [summary])
    upsert_fund_summaries(conn, [summary])
    count = conn.execute("SELECT COUNT(*) FROM fund_summaries").fetchone()[0]
    assert count == 1
