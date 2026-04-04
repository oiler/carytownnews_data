import logging
from datetime import datetime, timezone

from pipelines.shared.schema import Expenditure, FundSummary, NormalizedResult, Revenue

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _to_float(d: dict, key: str) -> float | None:
    v = d.get(key)
    return float(v) if v is not None else None


def normalize(
    raw: dict,
    source_file: str,
    doc_type: str,
    fiscal_year: int,
    quarter: int | None,
) -> NormalizedResult:
    """
    Convert Claude's JSON response to typed schema rows.
    Drops rows with missing required fields or null amounts.
    """
    now = _now()
    expenditures: list[Expenditure] = []
    revenues: list[Revenue] = []
    fund_summaries: list[FundSummary] = []

    for item in raw.get("expenditures", []) or []:
        dept = item.get("department")
        amount = item.get("amount")
        if not dept or amount is None:
            logger.debug(f"Dropping expenditure row with missing dept/amount: {item}")
            continue
        try:
            expenditures.append(Expenditure(
                pipeline="B",
                source_file=source_file,
                doc_type=doc_type,
                fiscal_year=fiscal_year,
                quarter=quarter,
                fund=item.get("fund") or "General",
                department=dept,
                division=item.get("division"),
                amount_type=item.get("amount_type") or "adopted",
                amount=float(amount),
                extracted_at=now,
            ))
        except (TypeError, ValueError) as e:
            logger.warning(f"Skipping malformed expenditure row {item}: {e}")

    for item in raw.get("revenues", []) or []:
        source = item.get("source")
        amount = item.get("amount")
        if not source or amount is None:
            logger.debug(f"Dropping revenue row with missing source/amount: {item}")
            continue
        try:
            revenues.append(Revenue(
                pipeline="B",
                source_file=source_file,
                doc_type=doc_type,
                fiscal_year=fiscal_year,
                quarter=quarter,
                fund=item.get("fund") or "General",
                source=source,
                amount_type=item.get("amount_type") or "adopted",
                amount=float(amount),
                extracted_at=now,
            ))
        except (TypeError, ValueError) as e:
            logger.warning(f"Skipping malformed revenue row {item}: {e}")

    for item in raw.get("fund_summaries", []) or []:
        fund = item.get("fund")
        if not fund:
            continue

        fund_summaries.append(FundSummary(
            pipeline="B",
            source_file=source_file,
            doc_type=doc_type,
            fiscal_year=fiscal_year,
            quarter=quarter,
            fund=fund,
            total_revenues=_to_float(item, "total_revenues"),
            total_expenditures=_to_float(item, "total_expenditures"),
            transfers_in=_to_float(item, "transfers_in"),
            transfers_out=_to_float(item, "transfers_out"),
            beginning_balance=_to_float(item, "beginning_balance"),
            ending_balance=_to_float(item, "ending_balance"),
            extracted_at=now,
        ))

    return NormalizedResult(
        expenditures=expenditures,
        revenues=revenues,
        fund_summaries=fund_summaries,
    )
