"""
Pipeline B entry point.

Usage:
    uv run pipelines/pipeline_b/run.py
    uv run pipelines/pipeline_b/run.py --file resources/budgets/2025.pdf
    uv run pipelines/pipeline_b/run.py --dry-run
"""
import argparse
import json
import logging
import os
from pathlib import Path

from anthropic import Anthropic

from pipelines.pipeline_a.page_finder import find_section_pages
from pipelines.pipeline_b.claude_extractor import extract_page
from pipelines.pipeline_b.normalizer import normalize
from pipelines.pipeline_b.page_map import get_anchors
from pipelines.pipeline_b.prompts import get_prompt
from pipelines.pipeline_b.renderer import render_page
from pipelines.shared.db import (
    create_schema,
    get_connection,
    upsert_expenditures,
    upsert_fund_summaries,
    upsert_revenues,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Haiku pricing per 1M tokens (as of April 2026)
_INPUT_COST_PER_M = 0.80
_OUTPUT_COST_PER_M = 4.00

RESOURCE_DIRS = {
    "budgets": "budget",
    "financial-reports": "acfr",
    "quarterly-reports": "quarterly",
}


def parse_filename(path: Path) -> tuple[str, int, int | None]:
    doc_type = RESOURCE_DIRS.get(path.parent.name)
    if doc_type is None:
        raise ValueError(f"Unknown resource directory: {path.parent.name}")
    stem = path.stem
    if doc_type == "quarterly":
        if "-q" in stem:
            year_str, q_str = stem.split("-q", 1)
            return doc_type, int(year_str), int(q_str)
        elif stem.endswith("-annual"):
            return doc_type, int(stem.removesuffix("-annual")), None
        else:
            raise ValueError(f"Cannot parse quarterly filename: {stem}")
    return doc_type, int(stem), None


def _compute_cost(input_tokens: int, output_tokens: int) -> float:
    return (input_tokens / 1_000_000) * _INPUT_COST_PER_M + \
           (output_tokens / 1_000_000) * _OUTPUT_COST_PER_M


def process_file(pdf_path: Path, client: Anthropic, conn, dry_run: bool = False) -> dict:
    doc_type, fiscal_year, quarter = parse_filename(pdf_path)
    anchors = get_anchors(doc_type)
    page_map = find_section_pages(str(pdf_path), anchors)
    prompt = get_prompt(doc_type)

    total_input_tokens = 0
    total_output_tokens = 0
    counts = {"expenditures": 0, "revenues": 0, "fund_summaries": 0}
    log_dir = Path("logs")

    for section, pages in page_map.items():
        for page_num in pages:
            try:
                png = render_page(str(pdf_path), page_num)
                if dry_run:
                    logger.info(f"  [dry-run] would send {pdf_path.name} p{page_num} ({len(png)} bytes)")
                    continue

                result = extract_page(png, prompt, client)
                total_input_tokens += result.input_tokens
                total_output_tokens += result.output_tokens

                # Log raw response for debugging
                log_dir.mkdir(exist_ok=True)
                log_path = log_dir / f"pipeline_b_{pdf_path.stem}_p{page_num}.json"
                log_path.write_text(json.dumps(result.data, indent=2))

                normalized = normalize(result.data, str(pdf_path), doc_type, fiscal_year, quarter)
                upsert_expenditures(conn, normalized.expenditures)
                upsert_revenues(conn, normalized.revenues)
                upsert_fund_summaries(conn, normalized.fund_summaries)
                counts["expenditures"] += len(normalized.expenditures)
                counts["revenues"] += len(normalized.revenues)
                counts["fund_summaries"] += len(normalized.fund_summaries)

            except Exception as e:
                logger.error(f"  Failed p{page_num} of {pdf_path}: {e}")

    cost = _compute_cost(total_input_tokens, total_output_tokens)
    return {**counts, "input_tokens": total_input_tokens, "output_tokens": total_output_tokens, "cost_usd": cost}


def discover_pdfs(resource_root: Path = Path("resources")) -> list[Path]:
    return sorted(resource_root.rglob("*.pdf"))


def main(args: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Pipeline B: Claude vision extraction")
    parser.add_argument("--file", help="Process a single PDF file")
    parser.add_argument("--dry-run", action="store_true",
                        help="Render pages and estimate cost without making API calls")
    parsed = parser.parse_args(args)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key and not parsed.dry_run:
        raise SystemExit("ANTHROPIC_API_KEY not set. Add it to .env and run: source .env")

    client = Anthropic(api_key=api_key or "dry-run")
    conn = get_connection()
    create_schema(conn)

    pdfs = [Path(parsed.file)] if parsed.file else discover_pdfs()
    total_cost = 0.0
    total_errors = 0

    for pdf_path in pdfs:
        logger.info(f"Processing {pdf_path}")
        try:
            result = process_file(pdf_path, client, conn, dry_run=parsed.dry_run)
            total_cost += result.get("cost_usd", 0)
            logger.info(f"  rows: {result}  cumulative cost: ${total_cost:.4f}")
        except Exception as e:
            logger.error(f"  Failed {pdf_path}: {e}")
            total_errors += 1

    print(f"\nPipeline B complete. Total cost: ${total_cost:.4f}. Errors: {total_errors}")


if __name__ == "__main__":
    main()
