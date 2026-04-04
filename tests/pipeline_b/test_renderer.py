import fitz
import pytest
from pathlib import Path
from pipelines.pipeline_b.renderer import render_page

BUDGET_PDF = Path("resources/budgets/2025.pdf")
PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


@pytest.mark.slow
def test_render_page_returns_bytes():
    result = render_page(str(BUDGET_PDF), 68)
    assert isinstance(result, bytes)
    assert len(result) > 0


@pytest.mark.slow
def test_render_page_returns_valid_png():
    result = render_page(str(BUDGET_PDF), 68)
    assert result[:8] == PNG_MAGIC, "Expected PNG file signature"


@pytest.mark.slow
def test_render_page_different_pages_produce_different_bytes():
    page_68 = render_page(str(BUDGET_PDF), 68)
    page_92 = render_page(str(BUDGET_PDF), 92)
    assert page_68 != page_92


def test_render_page_invalid_pdf_raises():
    with pytest.raises(fitz.FileNotFoundError):
        render_page("nonexistent.pdf", 1)


@pytest.mark.slow
def test_render_page_invalid_page_number_raises():
    with pytest.raises(ValueError, match="page"):
        render_page(str(BUDGET_PDF), 99999)
