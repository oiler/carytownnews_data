import fitz  # pymupdf


def render_page(pdf_path: str, page_number: int, dpi: int = 150) -> bytes:
    """
    Render a single PDF page (1-indexed) to PNG bytes at the given DPI.
    Raises ValueError if page_number exceeds the document length.
    Raises fitz.FileNotFoundError if pdf_path does not exist.
    """
    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    if page_number < 1 or page_number > total_pages:
        doc.close()
        raise ValueError(
            f"page {page_number} out of range for {pdf_path} ({total_pages} pages)"
        )
    page = doc[page_number - 1]
    zoom = dpi / 72  # pymupdf default DPI is 72
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat)
    png_bytes = pix.tobytes("png")
    doc.close()
    return png_bytes
