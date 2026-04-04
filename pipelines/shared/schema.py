from dataclasses import dataclass
from datetime import datetime


@dataclass
class Expenditure:
    pipeline: str
    source_file: str
    doc_type: str
    fiscal_year: int
    quarter: int | None
    fund: str
    department: str
    division: str | None
    amount_type: str
    amount: float
    extracted_at: datetime


@dataclass
class Revenue:
    pipeline: str
    source_file: str
    doc_type: str
    fiscal_year: int
    quarter: int | None
    fund: str
    source: str
    amount_type: str
    amount: float
    extracted_at: datetime


@dataclass
class FundSummary:
    pipeline: str
    source_file: str
    doc_type: str
    fiscal_year: int
    quarter: int | None
    fund: str
    total_revenues: float | None
    total_expenditures: float | None
    transfers_in: float | None
    transfers_out: float | None
    beginning_balance: float | None
    ending_balance: float | None
    extracted_at: datetime


@dataclass
class NormalizedResult:
    expenditures: list[Expenditure]
    revenues: list[Revenue]
    fund_summaries: list[FundSummary]
