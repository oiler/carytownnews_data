"""
Pipeline A entry point.

Usage:
    uv run pipelines/pipeline_a/run.py
    uv run pipelines/pipeline_a/run.py --file resources/budgets/2025.pdf
"""
import argparse
import logging
import sys
from pathlib import Path

from pipelines.pipeline_a.extractor import extract_text_pages
from pipelines.pipeline_a.normalizer import normalize_budget, normalize_quarterly, normalize_acfr
from pipelines.pipeline_a.page_finder import find_section_pages
from pipelines.pipeline_a.page_map import get_anchors
from pipelines.shared.db import create_schema, get_connection, upsert_expenditures, upsert_revenues, upsert_fund_summaries

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_RESOURCE_DIR_TO_DOC_TYPE = {
    "budgets": "budget",
    "financial-reports": "acfr",
    "quarterly-reports": "quarterly",
}


def parse_filename(path: Path) -> tuple[str, int, int | None]:
    """
    Infer (doc_type, fiscal_year, quarter) from a resource file path.

    Examples:
        resources/budgets/2025.pdf          → ('budget', 2025, None)
        resources/financial-reports/2024.pdf → ('acfr', 2024, None)
        resources/quarterly-reports/2024-q3.pdf → ('quarterly', 2024, 3)
        resources/quarterly-reports/2024-annual.pdf → ('quarterly', 2024, None)
    """
    doc_type = _RESOURCE_DIR_TO_DOC_TYPE.get(path.parent.name)
    if doc_type is None:
        raise ValueError(f"Unknown resource directory: {path.parent.name!r}")
    stem = path.stem
    if doc_type == "quarterly":
        if "-q" in stem:
            year_str, q_str = stem.split("-q", 1)
            return doc_type, int(year_str), int(q_str)
        elif stem.endswith("-annual"):
            return doc_type, int(stem.removesuffix("-annual")), None
        else:
            raise ValueError(f"Cannot parse quarterly filename: {stem!r}")
    return doc_type, int(stem), None


def process_file(pdf_path: Path, conn) -> dict[str, int]:
    """
    Run the full extraction pipeline on a single PDF.
    Returns counts dict: {'expenditures': N, 'revenues': N, 'fund_summaries': N}.
    """
    doc_type, fiscal_year, quarter = parse_filename(pdf_path)
    anchors = get_anchors(doc_type)
    section_pages = find_section_pages(str(pdf_path), anchors)

    # Collect all page numbers across all sections
    all_pages = sorted({p for pages in section_pages.values() for p in pages})
    text_pages = extract_text_pages(str(pdf_path), all_pages)

    if doc_type == "budget":
        result = normalize_budget(text_pages, str(pdf_path), fiscal_year)
    elif doc_type == "acfr":
        result = normalize_acfr(text_pages, str(pdf_path), fiscal_year)
    elif doc_type == "quarterly":
        result = normalize_quarterly(text_pages, str(pdf_path), fiscal_year, quarter)
    else:
        raise ValueError(f"Unknown doc_type: {doc_type!r}")

    upsert_expenditures(conn, result.expenditures)
    upsert_revenues(conn, result.revenues)
    upsert_fund_summaries(conn, result.fund_summaries)

    return {
        "expenditures": len(result.expenditures),
        "revenues": len(result.revenues),
        "fund_summaries": len(result.fund_summaries),
    }


def discover_pdfs(resource_root: Path = Path("resources")) -> list[Path]:
    """Return all PDF files under resources/, sorted."""
    return sorted(resource_root.rglob("*.pdf"))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Pipeline A: PDF text extraction")
    parser.add_argument("--file", help="Process a single PDF file instead of all")
    args = parser.parse_args(argv)

    conn = get_connection()
    create_schema(conn)

    pdfs = [Path(args.file)] if args.file else discover_pdfs()
    totals: dict[str, int] = {"expenditures": 0, "revenues": 0, "fund_summaries": 0, "errors": 0}

    for pdf_path in pdfs:
        logger.info(f"Processing {pdf_path}")
        try:
            counts = process_file(pdf_path, conn)
            for k, v in counts.items():
                totals[k] += v
            logger.info(f"  → {counts}")
        except Exception as e:
            logger.error(f"  ✗ Failed {pdf_path}: {e}")
            totals["errors"] += 1

    print(f"\nPipeline A complete: {totals}")


if __name__ == "__main__":
    main()
