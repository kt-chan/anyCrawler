import os
from pathlib import Path
import re
import shutil
import threading
from bs4 import ResultSet
import requests
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Callable
from urllib.parse import urlparse
import time


def get_root_scheme_domain_from_url(url: str):
    parsed = urlparse(url)
    http_scheme = parsed.scheme
    domain = parsed.netloc
    return http_scheme + "://" + domain


def get_root_domain_from_url(url: str):
    parsed = urlparse(url)
    # Extract domain and path parts
    domain = parsed.netloc
    return domain


def sanitize_name(name: str) -> str:
    """Sanitize a string to be safe for filesystem by replacing problematic characters."""
    # Replace non-alphanumeric, dots, hyphens, underscores with underscores
    return re.sub(r"[^\w.-]", "_", name)


def get_save_path_from_url(url: str, save_base_path: str) -> str:
    """Create directory structure based on URL and return full save path."""
    parsed = urlparse(url)
    domain = parsed.netloc
    path_parts = parsed.path.strip("/").split("/")

    # Remove filename from path parts if present
    if path_parts and "." in path_parts[-1]:
        filename = path_parts.pop()
    else:
        filename = "download"

    # Sanitize domain and each path part
    domain = sanitize_name(domain)
    path_parts = [sanitize_name(part) for part in path_parts]

    # Sanitize filename while preserving extension
    if "." in filename:
        name, ext = filename.rsplit(".", 1)
        filename = f"{sanitize_name(name)}.{sanitize_name(ext)}"
    else:
        filename = sanitize_name(filename)

    # Create full directory path
    full_dir = os.path.join(save_base_path, domain, *path_parts)
    os.makedirs(full_dir, exist_ok=True)

    return os.path.join(full_dir, filename)


def url_formater(url: str, tags: list[str], base_dir: str) -> list[str]:
    parsed = urlparse(url)
    http_scheme = parsed.scheme
    domain = parsed.netloc
    tag_urls = []
    for tag in tags:
        try:
            if tag.get("src", "").startswith("/"):
                ori_url = str(tag["src"])
                new_url = str(
                    Path(
                        get_save_path_from_url(ori_url, str(Path(base_dir) / domain))
                    ).resolve()
                )
                tag["src"] = new_url
                target_url = http_scheme + "://" + domain + ori_url
                tag_urls.append(target_url)
            if tag.get("href", "").startswith("/"):
                ori_url = str(tag["href"])
                new_url = str(
                    Path(
                        get_save_path_from_url(ori_url, str(Path(base_dir) / domain))
                    ).resolve()
                )
                tag["href"] = new_url
                target_url = http_scheme + "://" + domain + ori_url
                tag_urls.append(target_url)
        except Exception as e:
            print(f"error in parsing img tag: {e}")

    return tag_urls


def get_all_pdfs_in_temp_directory(storage_dir: str):
    pdfs = []

    try:
        for root, dirs, files in os.walk(storage_dir):
            for file in files:
                if file.endswith(".pdf"):
                    pdfs.append(str(Path(storage_dir + "/" + file).resolve()))
        if len(pdfs) == 0:
            return []
    except Exception as e:
        print(f"error in get_all_pdfs_from_html: {e}")

    return pdfs


def get_all_pdfs_from_directory(domain_url: str, storage_dir: str):
    domain_url = get_root_domain_from_url(domain_url)
    target_data_dir = str(Path(storage_dir + "/" + domain_url).resolve())
    pdfs = []

    try:
        for root, dirs, files in os.walk(target_data_dir):
            for file in files:

                # Skip working directory
                skip = str(Path(file).parent).endswith("pdf-upload.tmp") or str(
                    Path(file).parent
                ).endswith("pdf-upload.done")

                if file.endswith(".pdf") and not skip:
                    pdfs.append(os.path.join(root, file))

        if len(pdfs) == 0:
            return []

        tmp_upload_dir = Path(storage_dir).parent / "pdf-upload.tmp"
        os.makedirs(tmp_upload_dir, exist_ok=True)

        for idx, file in enumerate(pdfs):
            try:
                file_name = str(Path(file).parent.name) + "-" + str(Path(file).name)

                file_destination = str(tmp_upload_dir / file_name)

                shutil.move(file, file_destination)
                pdfs[idx] = file_destination
            except Exception as e:
                print(f"error in rename pdf {file}: {e}")
        try:
            shutil.rmtree(Path(target_data_dir) / "pdf-upload.done")
        except Exception as e:
            pass
    except Exception as e:
        print(f"error in get_all_pdfs_from_html: {e}")

    return pdfs


def set_pdfs_upload_done(urls: list[str], storage_dir: str):
    try:

        target_dir = Path(storage_dir) / "pdf-upload.done"
        os.makedirs(target_dir, exist_ok=True)

        for idx, file in enumerate(urls):
            try:
                file_destination = target_dir / Path(file).name
                shutil.move(file, file_destination)
            except Exception as e:
                print(f"error in rename pdf {file}: {e}")
    except Exception as e:
        print(f"error in set_all_pdfs_upload_done: {e}")

    print(f"RAG Loading Completed!")
    return True


class FileProcessor:
    def __init__(self, max_workers: int = 10, timeout: int = 30):
        """
        Initialize the PDF processor with thread pool and synchronization.

        Args:
            max_workers: Maximum number of concurrent download threads
            timeout: Request timeout in seconds
        """
        self.max_workers = max_workers
        self.timeout = timeout
        self.download_lock = threading.Lock()
        self.session = requests.Session()
        # Set default headers to mimic a browser
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
        )

    def batch_process_files(
        self,
        urls: List[str],
        progress_function: Callable[[str, str, Optional[Callable]], Optional[bool]],
        save_base_path: Optional[str] = None,
    ) -> List[str]:
        """
        Download multiple PDFs concurrently using thread pool.

        Args:
            urls: List of URLs to download
            save_base_path: Base directory to save the PDFs
            progress_callback: Optional callback function for progress updates

        Returns:
            List of successfully downloaded file paths
        """
        successful_processed = []

        # Use thread pool for concurrent downloads
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Create a future for each download task
            if save_base_path is None:
                future_to_url = {
                    executor.submit(
                        progress_function,
                        url,
                    ): url
                    for url in urls
                }

            if save_base_path is not None:
                future_to_url = {
                    executor.submit(
                        progress_function,
                        url,
                        get_save_path_from_url(url, save_base_path),
                    ): url
                    for url in urls
                }

            # Process completed downloads as they finish
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    success = future.result()
                    if success:
                        if save_base_path is None:
                            successful_processed.append(url)
                        if save_base_path is not None:
                            successful_processed.append(
                                get_save_path_from_url(url, save_base_path)
                            )
                except Exception as e:
                    raise e

        return successful_processed

    def close(self):
        """Clean up resources."""
        self.session.close()
