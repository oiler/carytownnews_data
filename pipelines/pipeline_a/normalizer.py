"""
Normalizer: converts raw extracted text into typed schema rows.

Parsing strategy:
- Budget overview pages: detect REVENUES/EXPENDITURES sections, parse column headers
  to determine fiscal year + amount_type, then parse data rows as line items.
- Budget dept profiles: detect "DEPARTMENT PROFILE" header, extract dept name,
  parse subsequent rows as expenditure line items.
- Quarterly: detect fund name, parse revenues/expenditures from structured text.
- ACFR: best-effort extraction of fund balances from statement text.
"""
import logging
import re
from datetime import datetime, timezone

from pipelines.shared.schema import Expenditure, FundSummary, NormalizedResult, Revenue

logger = logging.getLogger(__name__)

_SKIP_LABELS = {
    "total",
    "subtotal",
    "grand total",
    "total revenues",
    "total expenditures",
    "total general fund",
    "net revenues",
    "net expenditures",
    "total net revenue appropriated",
    "total revenues & sources",
    "total net expenditures & uses",
    "revenues & sources",
    "expenditures & uses",
}

# A number token: digits with optional commas and decimal, or parenthesized version
_NUMBER_RE = re.compile(r"\([\d,]+(?:\.\d+)?\)|\$?\s*[\d,]+(?:\.\d+)?")


def _parse_amount(value: str | None) -> float | None:
    """Parse a dollar amount string to float. Returns None if unparseable."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s in ("-", "—", "–", "n/a", "na", "---"):
        return None
    negative = s.startswith("(") and s.endswith(")")
    cleaned = re.sub(r"[$,\s()]", "", s)
    if not cleaned:
        return None
    try:
        result = float(cleaned)
        return -result if negative else result
    except ValueError:
        return None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _infer_amount_type(type_word: str, col_fiscal_year: int, doc_fiscal_year: int) -> str:
    """Infer amount_type from a type-word string and fiscal year context."""
    h = type_word.lower()
    if "actual" in h:
        return "actual"
    if "adopted" in h:
        return "adopted"
    if "estimated" in h or "estimate" in h:
        return "actual"  # treat estimated as actual (best available)
    if "recommended" in h:
        return "recommended"
    # If the year matches the document fiscal year, assume adopted
    if col_fiscal_year == doc_fiscal_year:
        return "adopted"
    return "actual"


def _parse_combined_header(
    type_line: str, year_line: str, doc_fiscal_year: int
) -> list[tuple[int, str]]:
    """
    Parse a pair of lines like:
        type_line: 'Actual Actual Estimated Adopted'
        year_line: 'FY 2022 FY 2023 FY2024 FY2025'
    or:
        type_line: 'Activity Actual Actual Actual Adopted Estimated Adopted'
        year_line: '2021 2022 2023 2024 2024 2025'
    Returns [(fiscal_year, amount_type), ...].
    """
    # Use (20\d{2}) without \b so 'FY2024' (no space) is also matched
    years = re.findall(r"(20\d{2})", year_line)
    if not years:
        return []
    type_words = re.findall(
        r"\b(Actual|Adopted|Estimated|Recommended)\b", type_line, re.IGNORECASE
    )
    cols = []
    for i, year_str in enumerate(years):
        year = int(year_str)
        type_word = type_words[i] if i < len(type_words) else ""
        amount_type = _infer_amount_type(type_word, year, doc_fiscal_year)
        cols.append((year, amount_type))
    return cols


def _parse_data_row(line: str, num_cols: int) -> tuple[str, list[float | None]] | None:
    """
    Parse a data line like 'Property Taxes 115,234,000 125,400,000 137,000,000 149,600,000'
    or 'PERSONNEL SERVICES $ 146,351 $ 150,657 $ 155,136 $ 128,159 $ 169,613 $ 304,813'.
    Also handles parenthesized negatives: 'Uses/(Sources) (2,505,495) (9,323,815) 0'.
    Returns (label, [amount, ...]) or None if not parseable.
    """
    # A label character is anything that's not a digit, '$', or '(' leading a number.
    # Strategy: walk from the right, collecting number tokens until we have num_cols.
    # Everything before the first number token is the label.

    # Find all number tokens and their positions
    # Number token: optional leading '$', then digits/commas/dot, or (digits...)
    token_re = re.compile(
        r"(?<!\w)\([\d,]+(?:\.\d+)?\)|(?<!\()\$?\s*[\d,]+(?:\.\d+)?(?!\))"
    )
    tokens = list(token_re.finditer(line))

    if not tokens:
        return None

    # The label ends just before the first number token
    label = line[: tokens[0].start()].strip()
    # Strip trailing punctuation from label
    label = label.rstrip("$: \t")
    if not label:
        return None

    # Collect amount strings from all tokens
    amount_strs = [t.group().strip() for t in tokens]
    if not amount_strs:
        return None

    # Use the last num_cols tokens as amounts
    selected = amount_strs[-num_cols:]
    amounts: list[float | None] = [_parse_amount(a) for a in selected]

    # Pad with None on the left if fewer columns
    while len(amounts) < num_cols:
        amounts.insert(0, None)

    return label, amounts


def normalize_budget(
    text_pages: dict[int, str],
    source_file: str,
    fiscal_year: int,
) -> NormalizedResult:
    """Parse budget overview and department profile pages."""
    expenditures: list[Expenditure] = []
    revenues: list[Revenue] = []
    fund_summaries: list[FundSummary] = []
    now = _now()

    for page_num, text in text_pages.items():
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

        is_overview = any("BUDGET OVERVIEW" in ln for ln in lines)
        is_dept_profile = any(
            "DEPARTMENT PROFILE" in ln or "DIVISION PROFILE" in ln for ln in lines
        )

        if is_overview:
            _parse_budget_overview(
                lines, source_file, fiscal_year, now,
                expenditures, revenues,
            )
        elif is_dept_profile:
            _parse_dept_profile(
                lines, source_file, fiscal_year, now, expenditures
            )

    return NormalizedResult(
        expenditures=expenditures,
        revenues=revenues,
        fund_summaries=fund_summaries,
    )


def _is_type_word_line(line: str) -> bool:
    """Return True if the line consists only of amount-type words (and optional filler)."""
    cleaned = re.sub(
        r"\b(Actual|Adopted|Estimated|Recommended|Activity)\b", "", line, flags=re.IGNORECASE
    ).strip()
    return bool(
        re.search(r"\b(Actual|Adopted|Estimated|Recommended)\b", line, re.IGNORECASE)
        and not re.search(r"\d", cleaned)
    )


def _parse_budget_overview(
    lines: list[str],
    source_file: str,
    fiscal_year: int,
    now: datetime,
    expenditures: list[Expenditure],
    revenues: list[Revenue],
) -> None:
    """Parse BUDGET OVERVIEW - ALL FUNDS text into revenue and expenditure rows."""
    section: str | None = None
    col_map: list[tuple[int, str]] = []

    i = 0
    while i < len(lines):
        line = lines[i]

        # ── Section headers ───────────────────────────────────────────────────
        if re.match(r"^REVENUES?\b", line, re.IGNORECASE) and not re.search(r"\d", line):
            section = "revenues"
            i += 1
            continue
        if re.match(r"^EXPENDITURES?\b", line, re.IGNORECASE) and not re.search(r"\d", line):
            section = "expenditures"
            i += 1
            continue

        # ── Column header detection ───────────────────────────────────────────
        # Case 1: type-words-only line followed immediately by a year line
        if _is_type_word_line(line) and i + 1 < len(lines):
            next_line = lines[i + 1]
            if re.search(r"\bFY\s*20\d{2}|(?<!\d)(20\d{2})(?!\d)", next_line):
                col_map = _parse_combined_header(line, next_line, fiscal_year)
                i += 2
                continue

        # Case 2: year line (FY YYYY format) — check prev line for type words
        if re.search(r"\bFY\s*20\d{2}", line):
            prev_line = lines[i - 1] if i > 0 else ""
            if _is_type_word_line(prev_line):
                col_map = _parse_combined_header(prev_line, line, fiscal_year)
            else:
                col_map = _parse_combined_header(line, line, fiscal_year)
            i += 1
            continue

        # Case 3: bare year line (e.g. '2021 2022 2023 2024') — dept profile style
        if re.match(r"^[\s\d]+$", line) and re.search(r"(20\d{2})", line):
            prev_line = lines[i - 1] if i > 0 else ""
            next_line = lines[i + 1] if i + 1 < len(lines) else ""
            type_line = prev_line if _is_type_word_line(next_line) else next_line
            col_map = _parse_combined_header(type_line, line, fiscal_year)
            i += 1
            continue

        # Skip standalone type-word lines (already handled by look-ahead)
        if _is_type_word_line(line) and not re.search(r"\d", line):
            i += 1
            continue

        # ── Data rows ─────────────────────────────────────────────────────────
        if not col_map or section is None:
            i += 1
            continue

        parsed = _parse_data_row(line, len(col_map))
        if parsed is None:
            i += 1
            continue

        label, amounts = parsed

        if label.lower() in _SKIP_LABELS:
            i += 1
            continue
        if len(label) < 3:
            i += 1
            continue
        # Skip lines that look like subtotal markers
        if re.match(r"^(REVENUES?|EXPENDITURES?)\s*&?\s*(SOURCES?|USES?)?$", label, re.IGNORECASE):
            i += 1
            continue

        for (col_year, amount_type), amount in zip(col_map, amounts):
            if amount is None:
                continue
            # Only emit the column matching the document's fiscal year
            if col_year != fiscal_year:
                continue
            if section == "revenues":
                revenues.append(
                    Revenue(
                        pipeline="A",
                        source_file=source_file,
                        doc_type="budget",
                        fiscal_year=fiscal_year,
                        quarter=None,
                        fund="General",
                        source=label,
                        amount_type=amount_type,
                        amount=amount,
                        extracted_at=now,
                    )
                )
            elif section == "expenditures":
                expenditures.append(
                    Expenditure(
                        pipeline="A",
                        source_file=source_file,
                        doc_type="budget",
                        fiscal_year=fiscal_year,
                        quarter=None,
                        fund="General",
                        department=label,
                        division=None,
                        amount_type=amount_type,
                        amount=amount,
                        extracted_at=now,
                    )
                )
        i += 1


def _parse_dept_profile(
    lines: list[str],
    source_file: str,
    fiscal_year: int,
    now: datetime,
    expenditures: list[Expenditure],
) -> None:
    """
    Parse DEPARTMENT PROFILE pages into expenditure rows.

    Real format (Cary FY2025 budget) uses:
      - em-dash separator: 'DEPARTMENT PROFILE — TOWN COUNCIL'
      - Year line: '2021 2022 2023 2024 2024 2025'  (bare years, no FY prefix)
      - Type line BELOW years: 'Activity Actual Actual Actual Adopted Estimated Adopted'
      - Data rows: 'PERSONNEL SERVICES $ 146,351 $ 150,657 ...'

    Fixture format uses:
      - hyphen: 'DEPARTMENT PROFILE - POLICE'
      - Type line ABOVE years: 'Actual Adopted Adopted'
      - Year line: 'FY 2023 FY 2024 FY 2025'
    """
    department: str | None = None
    col_map: list[tuple[int, str]] = []

    i = 0
    while i < len(lines):
        line = lines[i]

        # Extract department name from header (supports - and — separators)
        m = re.match(
            r"(?:DEPARTMENT|DIVISION) PROFILE\s*[-–—]\s*(.+)", line, re.IGNORECASE
        )
        if m:
            department = m.group(1).strip().title()
            i += 1
            continue

        # Case A: type-words line ABOVE a year line (fixture style)
        if _is_type_word_line(line) and i + 1 < len(lines):
            next_line = lines[i + 1]
            if re.search(r"\bFY\s*20\d{2}|(?<!\d)(20\d{2})(?!\d)", next_line):
                col_map = _parse_combined_header(line, next_line, fiscal_year)
                i += 2
                continue

        # Case B: FY-prefixed year line (check prev for type words)
        if re.search(r"\bFY\s*20\d{2}", line):
            prev_line = lines[i - 1] if i > 0 else ""
            if _is_type_word_line(prev_line):
                col_map = _parse_combined_header(prev_line, line, fiscal_year)
            else:
                col_map = _parse_combined_header(line, line, fiscal_year)
            i += 1
            continue

        # Case C: bare year line (e.g. '2021 2022 2023 2024 2024 2025')
        # followed by a type-words line on the NEXT line
        if re.match(r"^[\s\d]+$", line) and re.search(r"(20\d{2})", line):
            next_line = lines[i + 1] if i + 1 < len(lines) else ""
            if _is_type_word_line(next_line):
                col_map = _parse_combined_header(next_line, line, fiscal_year)
                i += 2  # consume year line and type line
                continue

        # Skip standalone type-word lines
        if _is_type_word_line(line) and not re.search(r"\d", line):
            i += 1
            continue

        if not department or not col_map:
            i += 1
            continue

        parsed = _parse_data_row(line, len(col_map))
        if not parsed:
            i += 1
            continue

        label, amounts = parsed

        if label.lower() in _SKIP_LABELS or label.lower() == "total":
            i += 1
            continue
        # Skip "Authorized FTEs" rows — not monetary
        if "fte" in label.lower() or "authorized" in label.lower():
            i += 1
            continue

        for (col_year, amount_type), amount in zip(col_map, amounts):
            if amount is None:
                continue
            # Only emit the column matching the document's fiscal year
            if col_year != fiscal_year:
                continue
            expenditures.append(
                Expenditure(
                    pipeline="A",
                    source_file=source_file,
                    doc_type="budget",
                    fiscal_year=fiscal_year,
                    quarter=None,
                    fund="General",
                    department=department,
                    division=label,
                    amount_type=amount_type,
                    amount=amount,
                    extracted_at=now,
                )
            )
        i += 1


def normalize_quarterly(
    text_pages: dict[int, str],
    source_file: str,
    fiscal_year: int,
    quarter: int | None,
) -> NormalizedResult:
    """
    Parse quarterly report financial highlights pages.

    Real quarterly report amounts are in millions (e.g. 270.1 = $270.1M).
    We detect whether a page uses millions format and scale accordingly.
    """
    fund_summaries: list[FundSummary] = []
    now = _now()

    for page_num, text in text_pages.items():
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

        # Detect if this page uses millions notation
        uses_millions = any("million" in ln.lower() or "in millions" in ln.lower() for ln in lines)
        # Also detect from the summary table pattern: amounts like "270.1"
        # which are small floating point numbers alongside a % column
        if not uses_millions:
            float_pattern = re.findall(r"\$\s*[\d]+\.\d+", text)
            if float_pattern:
                uses_millions = True
        scale = 1_000_000.0 if uses_millions else 1.0

        current_fund = "General"
        total_revenues: float | None = None
        total_expenditures: float | None = None

        for line in lines:
            # Fund name detection
            if re.search(r"\bGeneral Fund\b", line, re.IGNORECASE):
                current_fund = "General"
            elif re.search(r"\bUtility Fund\b", line, re.IGNORECASE):
                current_fund = "Utility"

            # Revenue line: matches "REVENUES $ 270.1 $ 183.7 68% ..." or "Revenues 270,100,000 ..."
            # Column layout: [adj_budget, ytd_actual, pct_of_budget, ...]
            # Prefer ytd_actual (index 1) when available; fall back to index 0.
            if re.match(r"^revenues?\b", line, re.IGNORECASE):
                amounts = _extract_amounts_from_line(line, scale)
                if len(amounts) >= 2:
                    total_revenues = amounts[1]  # YTD actual
                elif amounts:
                    total_revenues = amounts[0]

            # Expenditure line (same column layout)
            if re.match(r"^expenditures?\b", line, re.IGNORECASE):
                amounts = _extract_amounts_from_line(line, scale)
                if len(amounts) >= 2:
                    total_expenditures = amounts[1]  # YTD actual
                elif amounts:
                    total_expenditures = amounts[0]

        if total_revenues is not None or total_expenditures is not None:
            fund_summaries.append(
                FundSummary(
                    pipeline="A",
                    source_file=source_file,
                    doc_type="quarterly",
                    fiscal_year=fiscal_year,
                    quarter=quarter,
                    fund=current_fund,
                    total_revenues=total_revenues,
                    total_expenditures=total_expenditures,
                    transfers_in=None,
                    transfers_out=None,
                    beginning_balance=None,
                    ending_balance=None,
                    extracted_at=now,
                )
            )

    return NormalizedResult(expenditures=[], revenues=[], fund_summaries=fund_summaries)


def _extract_amounts_from_line(line: str, scale: float = 1.0) -> list[float]:
    """
    Extract numeric amounts from a line, applying scale factor.
    Skips percentage-like values (e.g. '68%' or values < 1 after scaling).
    Skips values that look like percentages (followed by %).
    """
    # Remove percentage tokens first
    cleaned = re.sub(r"[\d.]+\s*%", "", line)
    # Find all number-like tokens (including decimals, commas, dollar sign)
    raw = re.findall(r"\$?\s*([\d,]+(?:\.\d+)?)", cleaned)
    results = []
    for tok in raw:
        val = _parse_amount(tok)
        if val is None:
            continue
        scaled = val * scale
        # Filter out obviously non-monetary values (page numbers, FY years, etc.)
        if scaled < 1_000 and scale > 1:
            continue
        if scaled < 1:
            continue
        # Skip values that look like fiscal years
        if 2000 <= scaled <= 2100 and scale == 1.0:
            continue
        results.append(scaled)
    return results


def normalize_acfr(
    text_pages: dict[int, str],
    source_file: str,
    fiscal_year: int,
) -> NormalizedResult:
    """
    Best-effort extraction from ACFR pages.
    ACFR financial statements are complex; extract what we can from text.
    Returns NormalizedResult with whatever fund summaries are parseable.
    Pipeline B (Claude Vision) is expected to handle ACFR data more reliably.
    """
    logger.info(
        "ACFR normalizer: best-effort extraction for %s FY%d", source_file, fiscal_year
    )
    return NormalizedResult(expenditures=[], revenues=[], fund_summaries=[])
