import fitz  # pymupdf


def render_page(pdf_path: str, page_number: int, dpi: int = 150) -> bytes:
    """
    Render a single PDF page (1-indexed) to PNG bytes at the given DPI.
    Raises ValueError if page_number exceeds the document length or dpi is not positive.
    Raises fitz.FileNotFoundError if pdf_path does not exist.
    """
    if dpi <= 0:
        raise ValueError(f"dpi must be positive, got {dpi}")
    with fitz.open(pdf_path) as doc:
        total_pages = len(doc)
        if page_number < 1 or page_number > total_pages:
            raise ValueError(
                f"page {page_number} out of range for {pdf_path} ({total_pages} pages)"
            )
        page = doc[page_number - 1]
        zoom = dpi / 72  # pymupdf default DPI is 72
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat)
        return pix.tobytes("png")
