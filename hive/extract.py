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
    import pytesseract
    from PIL import Image
except ImportError:
    pytesseract = None

TIMEOUT = 60
OCR_PAGE_LIMIT = 50
MIN_CHARS = 100

HAS_PDFTOTEXT = shutil.which("pdftotext") is not None


def extract_text(pdf_path: str, output_path: str) -> dict:
    """
    Extract text from PDF with three-tier fallback:
    1. pdftotext -layout (fastest, if installed)
    2. PyMuPDF text extraction (fast, pure Python)
    3. PyMuPDF render → Tesseract OCR (slow, for scanned docs)

    Returns: {"status": "done"|"failed", "method": str, "char_count": int, "error": str|None}
    """
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

    # Tier 3: OCR
    if fitz is not None and pytesseract is not None:
        result = _try_ocr(pdf_path, output_path)
        if result:
            return result

    return {
        "status": "failed",
        "method": None,
        "char_count": 0,
        "error": "All extraction methods failed",
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
    except Exception as e:
        return {
            "status": "failed",
            "method": "ocr",
            "char_count": 0,
            "error": str(e)[:200],
        }
    return None
