import os
import json
from typing import Tuple
import pdfkit

PDFKIT_CONFIG = pdfkit.configuration(
    wkhtmltopdf=r"D:\Apps\wkhtmltopdf\bin\wkhtmltopdf.exe"
)

PDFKIT_OPTION = {
    # 可添加allow、cookie、custom-header、post等参数
    "encoding": "utf-8",
    "enable-local-file-access": True,
    "page-size": "A4",  # 设置页面大小为A4
    "dpi": 500,  # 设置分辨率
    "margin-top": "10mm",  # 设置上边距
    "margin-left": "10mm",
    "margin-right": "10mm",
    "margin-bottom": "10mm",
    "no-outline": None,
}


def _save_html_file_from_json(file_path, data, save_path: str = None):
    chunk_size = 8192  # 8KB chunks
    file_path = save_path or file_path.replace("json", "html")
    with open(file_path, "wb") as f:
        print(f"Writing html file to {file_path} ...")
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size].encode(
                "utf-8"
            )  # Convert string chunk to bytes
            f.write(chunk)
    return file_path


def _save_pdf_file_from_html(file_path, save_path: str = None):
    pdf_file_path = save_path or str(file_path).replace(".html", ".pdf")
    pdfkit.from_file(
        file_path, pdf_file_path, configuration=PDFKIT_CONFIG, options=PDFKIT_OPTION
    )
    return pdf_file_path


def _save_file(file_path, data, save_path: str = None) -> bool:
    try:
        html_file_path = _save_html_file_from_json(file_path, data, save_path)
        pdf_file_path = _save_pdf_file_from_html(html_file_path, save_path)
    except Exception as e:
        raise (f"Error Saving {file_path}: {e}")

    return len(html_file_path) > 0 and len(pdf_file_path) > 0


def process_json_file(file_path, target_tag, save_path: str = None) -> bool:
    """Process a single JSON file."""
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
            if target_tag in data:
                html = str(data[target_tag])
                if len(html) > 0:
                    return _save_file(file_path, html, save_path)
    except json.JSONDecodeError as e:
        raise (f"Error decoding JSON from {file_path}: {e}")
    except Exception as e:
        raise (f"Error reading {file_path}: {e}")
