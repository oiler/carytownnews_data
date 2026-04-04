import sqlite3
from pathlib import Path
from .schema import Expenditure, Revenue, FundSummary

DB_PATH = Path("data/cary.db")

_CREATE_EXPENDITURES = """
CREATE TABLE IF NOT EXISTS expenditures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline TEXT NOT NULL,
    source_file TEXT NOT NULL,
    doc_type TEXT NOT NULL,
    fiscal_year INTEGER NOT NULL,
    quarter INTEGER,
    fund TEXT NOT NULL,
    department TEXT NOT NULL,
    division TEXT,
    amount_type TEXT NOT NULL,
    amount REAL NOT NULL,
    extracted_at TEXT NOT NULL
)
"""

# Expression index: COALESCE maps NULL to '' so NULL==NULL in the unique key.
_CREATE_EXPENDITURES_IDX = """
CREATE UNIQUE INDEX IF NOT EXISTS expenditures_uq
ON expenditures(
    pipeline, source_file, doc_type, fiscal_year, COALESCE(quarter, -1),
    fund, department, COALESCE(division, ''), amount_type
)
"""

_CREATE_REVENUES = """
CREATE TABLE IF NOT EXISTS revenues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline TEXT NOT NULL,
    source_file TEXT NOT NULL,
    doc_type TEXT NOT NULL,
    fiscal_year INTEGER NOT NULL,
    quarter INTEGER,
    fund TEXT NOT NULL,
    source TEXT NOT NULL,
    amount_type TEXT NOT NULL,
    amount REAL NOT NULL,
    extracted_at TEXT NOT NULL
)
"""

_CREATE_REVENUES_IDX = """
CREATE UNIQUE INDEX IF NOT EXISTS revenues_uq
ON revenues(
    pipeline, source_file, doc_type, fiscal_year, COALESCE(quarter, -1),
    fund, source, amount_type
)
"""

_CREATE_FUND_SUMMARIES = """
CREATE TABLE IF NOT EXISTS fund_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline TEXT NOT NULL,
    source_file TEXT NOT NULL,
    doc_type TEXT NOT NULL,
    fiscal_year INTEGER NOT NULL,
    quarter INTEGER,
    fund TEXT NOT NULL,
    total_revenues REAL,
    total_expenditures REAL,
    transfers_in REAL,
    transfers_out REAL,
    beginning_balance REAL,
    ending_balance REAL,
    extracted_at TEXT NOT NULL
)
"""

_CREATE_FUND_SUMMARIES_IDX = """
CREATE UNIQUE INDEX IF NOT EXISTS fund_summaries_uq
ON fund_summaries(
    pipeline, source_file, doc_type, fiscal_year, COALESCE(quarter, -1), fund
)
"""


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    conn.execute(_CREATE_EXPENDITURES)
    conn.execute(_CREATE_EXPENDITURES_IDX)
    conn.execute(_CREATE_REVENUES)
    conn.execute(_CREATE_REVENUES_IDX)
    conn.execute(_CREATE_FUND_SUMMARIES)
    conn.execute(_CREATE_FUND_SUMMARIES_IDX)
    conn.commit()


# On conflict, only update amount and extracted_at.
# Key fields (fund, department, etc.) are assumed stable across re-extractions.
def upsert_expenditures(conn: sqlite3.Connection, rows: list[Expenditure]) -> None:
    if not rows:
        return
    insert_sql = """
    INSERT OR IGNORE INTO expenditures
        (pipeline, source_file, doc_type, fiscal_year, quarter, fund,
         department, division, amount_type, amount, extracted_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    update_sql = """
    UPDATE expenditures
    SET amount=?, extracted_at=?
    WHERE pipeline=? AND source_file=? AND doc_type=? AND fiscal_year=?
      AND (quarter IS ?) AND fund=? AND department=?
      AND (division IS ?) AND amount_type=?
    """
    with conn:
        conn.executemany(insert_sql, [
            (
                r.pipeline, r.source_file, r.doc_type, r.fiscal_year, r.quarter,
                r.fund, r.department,
                r.division if r.division else None,  # normalize "" to None
                r.amount_type, r.amount, r.extracted_at.isoformat(),
            )
            for r in rows
        ])
        conn.executemany(update_sql, [
            (
                r.amount, r.extracted_at.isoformat(),
                r.pipeline, r.source_file, r.doc_type, r.fiscal_year, r.quarter,
                r.fund, r.department,
                r.division if r.division else None,  # normalize "" to None
                r.amount_type,
            )
            for r in rows
        ])


# On conflict, only update amount and extracted_at.
# Key fields (fund, source, etc.) are assumed stable across re-extractions.
def upsert_revenues(conn: sqlite3.Connection, rows: list[Revenue]) -> None:
    if not rows:
        return
    insert_sql = """
    INSERT OR IGNORE INTO revenues
        (pipeline, source_file, doc_type, fiscal_year, quarter, fund,
         source, amount_type, amount, extracted_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    update_sql = """
    UPDATE revenues
    SET amount=?, extracted_at=?
    WHERE pipeline=? AND source_file=? AND doc_type=? AND fiscal_year=?
      AND (quarter IS ?) AND fund=? AND source=? AND amount_type=?
    """
    with conn:
        conn.executemany(insert_sql, [
            (
                r.pipeline, r.source_file, r.doc_type, r.fiscal_year, r.quarter,
                r.fund, r.source, r.amount_type, r.amount, r.extracted_at.isoformat(),
            )
            for r in rows
        ])
        conn.executemany(update_sql, [
            (
                r.amount, r.extracted_at.isoformat(),
                r.pipeline, r.source_file, r.doc_type, r.fiscal_year, r.quarter,
                r.fund, r.source, r.amount_type,
            )
            for r in rows
        ])


# On conflict, only update amount fields and extracted_at.
# Key fields (fund, etc.) are assumed stable across re-extractions.
def upsert_fund_summaries(conn: sqlite3.Connection, rows: list[FundSummary]) -> None:
    if not rows:
        return
    insert_sql = """
    INSERT OR IGNORE INTO fund_summaries
        (pipeline, source_file, doc_type, fiscal_year, quarter, fund,
         total_revenues, total_expenditures, transfers_in, transfers_out,
         beginning_balance, ending_balance, extracted_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    update_sql = """
    UPDATE fund_summaries
    SET total_revenues=?, total_expenditures=?, transfers_in=?,
        transfers_out=?, beginning_balance=?, ending_balance=?, extracted_at=?
    WHERE pipeline=? AND source_file=? AND doc_type=? AND fiscal_year=?
      AND (quarter IS ?) AND fund=?
    """
    with conn:
        conn.executemany(insert_sql, [
            (
                r.pipeline, r.source_file, r.doc_type, r.fiscal_year, r.quarter, r.fund,
                r.total_revenues, r.total_expenditures, r.transfers_in, r.transfers_out,
                r.beginning_balance, r.ending_balance, r.extracted_at.isoformat(),
            )
            for r in rows
        ])
        conn.executemany(update_sql, [
            (
                r.total_revenues, r.total_expenditures, r.transfers_in, r.transfers_out,
                r.beginning_balance, r.ending_balance, r.extracted_at.isoformat(),
                r.pipeline, r.source_file, r.doc_type, r.fiscal_year, r.quarter, r.fund,
            )
            for r in rows
        ])
