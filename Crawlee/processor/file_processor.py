import os
from pathlib import Path
import re
import threading
from bs4 import ResultSet
import requests
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Callable
from urllib.parse import urlparse
import time


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

    def _download(
        self, url: str, save_path: str, progress_callback: Optional[Callable] = None
    ) -> bool:
        """Download a single PDF file with synchronization protection."""
        try:
            # Use a lock to prevent race conditions when accessing shared resources
            with self.download_lock:
                # Check if file already exists to avoid re-downloading
                if os.path.exists(save_path):
                    if progress_callback:
                        progress_callback(url, save_path, "already_exists", 0)
                    return True

            # Stream the download to handle large files efficiently
            response = self.session.get(url, stream=True, timeout=self.timeout)
            response.raise_for_status()

            # Get file size for progress tracking
            total_size = int(response.headers.get("content-length", 0))
            downloaded_size = 0

            # Download in chunks for efficiency
            chunk_size = 8192  # 8KB chunks
            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)

                        # Report progress if callback provided
                        if progress_callback and total_size > 0:
                            progress = (downloaded_size / total_size) * 100
                            progress_callback(url, save_path, "downloading", progress)

            if progress_callback:
                progress_callback(url, save_path, "completed", 100)

            return True

        except requests.exceptions.RequestException as e:
            if progress_callback:
                progress_callback(url, save_path, f"error: {str(e)}", 0)
            # Clean up partially downloaded file
            if os.path.exists(save_path):
                os.remove(save_path)
            return False
        except Exception as e:
            if progress_callback:
                progress_callback(url, save_path, f"error: {str(e)}", 0)
            return False

    def download_file(
        self,
        url: str,
        save_base_path: str = "downloads",
        progress_callback: Optional[Callable] = None,
    ) -> bool:
        """
        Download a single PDF from a URL.

        Args:
            url: URL of the PDF to download
            save_base_path: Base directory to save the PDF
            progress_callback: Optional callback function for progress updates

        Returns:
            bool: True if download was successful, False otherwise
        """
        # Generate save path with directory structure
        save_path = get_save_path_from_url(url, save_base_path)

        return self._download(url, save_path, progress_callback)

    def download_files(
        self,
        urls: List[str],
        save_base_path: str,
        progress_callback: Optional[Callable] = None,
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
        successful_downloads = []

        # Use thread pool for concurrent downloads
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Create a future for each download task
            future_to_url = {
                executor.submit(
                    self._download,
                    url,
                    get_save_path_from_url(url, save_base_path),
                    progress_callback,
                ): url
                for url in urls
            }

            # Process completed downloads as they finish
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    success = future.result()
                    if success:
                        successful_downloads.append(
                            get_save_path_from_url(url, save_base_path)
                        )
                except Exception as e:
                    if progress_callback:
                        progress_callback(url, "", f"error: {str(e)}", 0)

        return successful_downloads

    def close(self):
        """Clean up resources."""
        self.session.close()
