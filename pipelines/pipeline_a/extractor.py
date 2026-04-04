"""
Extractor: pulls raw content from PDF pages.

Two strategies:
- extract_text_pages(): for budget and ACFR pages where extract_table() fails.
  Returns {page_num: text_string}.
- extract_table_pages(): for quarterly reports where tables are machine-readable.
  Returns {page_num: [DataFrame, ...]}.

Both functions open the PDF once and close it cleanly, log failures per page,
and never raise — failed pages are silently omitted from the result.
"""
import logging

import pandas as pd
import pdfplumber

logger = logging.getLogger(__name__)


def extract_text_pages(pdf_path: str, pages: list[int]) -> dict[int, str]:
    """
    Extract text from the specified 1-indexed pages.
    Returns {page_number: text}. Pages with no text or errors are omitted.
    """
    result: dict[int, str] = {}
    with pdfplumber.open(pdf_path) as pdf:
        for page_num in pages:
            try:
                text = pdf.pages[page_num - 1].extract_text()
                if text and text.strip():
                    result[page_num] = text
                else:
                    logger.debug(f"No text on p{page_num} of {pdf_path}")
            except Exception as e:
                logger.error(f"Text extraction failed on p{page_num} of {pdf_path}: {e}")
    return result


def extract_table_pages(pdf_path: str, pages: list[int]) -> dict[int, list[pd.DataFrame]]:
    """
    Extract tables from the specified 1-indexed pages using extract_tables() (plural).
    Returns {page_number: [DataFrame, ...]}. Pages with no usable tables are omitted.
    Single-column tables (likely headers/footers) are filtered out.
    """
    result: dict[int, list[pd.DataFrame]] = {}
    with pdfplumber.open(pdf_path) as pdf:
        for page_num in pages:
            try:
                raw_tables = pdf.pages[page_num - 1].extract_tables()
                dfs = []
                for table in raw_tables:
                    if not table or len(table) < 2:
                        continue
                    df = pd.DataFrame(table[1:], columns=table[0])
                    if df.shape[1] <= 1:
                        continue  # skip single-column tables
                    dfs.append(df)
                if dfs:
                    result[page_num] = dfs
                else:
                    logger.debug(f"No usable tables on p{page_num} of {pdf_path}")
            except Exception as e:
                logger.error(f"Table extraction failed on p{page_num} of {pdf_path}: {e}")
    return result
