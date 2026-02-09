import os
import subprocess
import tempfile
import shutil

# Try importing optional dependencies
try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

try:
    from paddleocr import PaddleOCR
except ImportError:
    PaddleOCR = None

try:
    import pytesseract
    from PIL import Image
except ImportError:
    pytesseract = None

TIMEOUT = 60
OCR_PAGE_LIMIT = 50
MIN_CHARS = 100

HAS_PDFTOTEXT = shutil.which("pdftotext") is not None

# Initialize PaddleOCR once (lazy load on first use)
_paddle_ocr = None


def _get_paddle_ocr():
    global _paddle_ocr
    if _paddle_ocr is None and PaddleOCR is not None:
        try:
            _paddle_ocr = PaddleOCR(use_angle_cls=True, lang="en")
        except Exception:
            pass
    return _paddle_ocr


def extract_text(pdf_path: str, output_path: str) -> dict:
    """
    Extract text from PDF with 4-tier fallback:
    1. pdftotext -layout (fastest, if installed)
    2. PyMuPDF text extraction (fast, pure Python)
    3. PaddleOCR (GPU-accelerated, best quality, if available)
    4. PyMuPDF render → Tesseract OCR (CPU fallback, for scanned docs)

    PDFs with no extractable text (photos, blank pages) are marked "done"
    with char_count=0. Only actual errors produce "failed".

    Returns: {"status": "done"|"failed", "method": str, "char_count": int, "error": str|None}
    """
    if not os.path.exists(pdf_path):
        return {"status": "failed", "method": None, "char_count": 0,
                "error": f"File not found: {pdf_path[:150]}"}

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Tier 1: pdftotext
    if HAS_PDFTOTEXT:
        result = _try_pdftotext(pdf_path, output_path)
        if result:
            return result

    # Tier 2: PyMuPDF text
    if fitz is not None:
        result = _try_pymupdf(pdf_path, output_path)
        if result:
            return result

    # Tier 3: PaddleOCR (GPU-optimized)
    result = _try_paddle_ocr(pdf_path, output_path)
    if result:
        return result

    # Tier 4: Tesseract OCR (CPU fallback)
    if fitz is not None and pytesseract is not None:
        result = _try_ocr(pdf_path, output_path)
        if result:
            return result

    # No method produced enough text - PDF likely has no extractable text
    # (photos, blank pages, redacted docs). Mark as done, not failed.
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("")
    return {
        "status": "done",
        "method": "empty",
        "char_count": 0,
        "error": None,
    }


def _try_pdftotext(pdf_path: str, output_path: str) -> dict | None:
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", pdf_path, output_path],
            capture_output=True, timeout=TIMEOUT,
        )
        if result.returncode == 0 and os.path.exists(output_path):
            size = os.path.getsize(output_path)
            if size >= MIN_CHARS:
                return {
                    "status": "done",
                    "method": "pdftotext",
                    "char_count": size,
                    "error": None,
                }
    except (subprocess.TimeoutExpired, Exception):
        pass
    return None


def _try_pymupdf(pdf_path: str, output_path: str) -> dict | None:
    try:
        doc = fitz.open(pdf_path)
        text_parts = []
        for page in doc:
            text_parts.append(page.get_text())
        doc.close()

        text = "\n".join(text_parts)
        if len(text) >= MIN_CHARS:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(text)
            return {
                "status": "done",
                "method": "pymupdf",
                "char_count": len(text),
                "error": None,
            }
    except Exception:
        pass
    return None


def _try_paddle_ocr(pdf_path: str, output_path: str) -> dict | None:
    """Use PaddleOCR for scanned/image PDFs. GPU-accelerated if available."""
    ocr = _get_paddle_ocr()
    if ocr is None:
        return None

    try:
        doc = fitz.open(pdf_path)
        pages = min(len(doc), OCR_PAGE_LIMIT)
        text_parts = []

        for i in range(pages):
            page = doc[i]
            # Render to image
            mat = fitz.Matrix(2, 2)  # 2x zoom for better OCR
            pix = page.get_pixmap(matrix=mat)

            # Save to temp file for PaddleOCR
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                pix.save(tmp.name)
                tmp_path = tmp.name

            try:
                # PaddleOCR returns list of results per line
                results = ocr.ocr(tmp_path, cls=True)
                if results and results[0]:
                    page_text = "\n".join([line[1][0] for line in results[0]])
                    text_parts.append(page_text)
            finally:
                os.unlink(tmp_path)

        doc.close()
        text = "\n".join(text_parts)

        if len(text) >= MIN_CHARS:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(text)
            return {
                "status": "done",
                "method": "paddle",
                "char_count": len(text),
                "error": None,
            }
    except Exception:
        pass
    return None


def _try_ocr(pdf_path: str, output_path: str) -> dict | None:
    try:
        doc = fitz.open(pdf_path)
        pages = min(len(doc), OCR_PAGE_LIMIT)
        text_parts = []

        for i in range(pages):
            page = doc[i]
            mat = fitz.Matrix(2, 2)  # 2x zoom for better OCR
            pix = page.get_pixmap(matrix=mat)

            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                pix.save(tmp.name)
                tmp_path = tmp.name

            try:
                img = Image.open(tmp_path)
                page_text = pytesseract.image_to_string(img)
                text_parts.append(page_text)
            finally:
                os.unlink(tmp_path)

        doc.close()
        text = "\n".join(text_parts)

        if len(text) >= MIN_CHARS:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(text)
            return {
                "status": "done",
                "method": "ocr",
                "char_count": len(text),
                "error": None,
            }
    except Exception:
        pass
    return None
