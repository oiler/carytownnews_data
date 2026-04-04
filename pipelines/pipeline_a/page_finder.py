import logging
import pdfplumber
from .page_map import SectionAnchor

logger = logging.getLogger(__name__)


def find_section_pages(pdf_path: str, anchors: list[SectionAnchor]) -> dict[str, list[int]]:
    """
    Scan a PDF and return the 1-indexed page numbers for each anchor section.

    For each anchor:
    - If collect_all=False: find the FIRST page containing the keyword,
      then include that page + the next `pages_after` pages.
    - If collect_all=True: find ALL pages containing the keyword.

    Keyword matching is case-sensitive. Keywords in page_map.py use the exact
    casing found in the PDF text (typically ALL-CAPS section headers).

    Returns {section_name: [page_numbers, ...]}.
    Pages with no text (e.g. image-only pages) are skipped silently.
    """
    result: dict[str, list[int]] = {a.section: [] for a in anchors}

    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        page_texts: list[tuple[int, str]] = []

        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            page_texts.append((i, text))

    for anchor in anchors:
        found: list[int] = []

        if anchor.collect_all:
            for page_num, text in page_texts:
                if anchor.keyword in text:
                    found.append(page_num)
        else:
            for page_num, text in page_texts:
                if anchor.keyword in text:
                    # Include this page + pages_after subsequent pages
                    end = min(page_num + anchor.pages_after, total)
                    found = list(range(page_num, end + 1))
                    break  # first match only

        if not found:
            logger.warning(f"Keyword {anchor.keyword!r} not found in {pdf_path}")

        result[anchor.section] = found

    return result
