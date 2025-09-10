from __future__ import annotations
import os
import json
import asyncio
from dotenv import load_dotenv
from datetime import timedelta
from pathlib import Path
import shutil
import time
from typing import Callable, Dict, Any, Optional
from urllib.parse import urljoin

import requests
from util.random_id_factory import RandomIDFactory, FileMetadata
from util.htmlfile_writer import process_json_file
from processor.file_processor import (
    FileProcessor,
    get_root_scheme_domain_from_url,
    get_root_domain_from_url,
    get_save_path_from_url,
    url_formater,
    get_all_pdfs_from_directory,
    get_all_pdfs_in_temp_directory,
    set_pdfs_upload_done,
)
from urllib.parse import urlparse
from crawlee import (
    Glob,
    HttpHeaders,
    RequestOptions,
    RequestTransformAction,
    SkippedReason,
)
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from processor.rag_processor import upload_document

# Set the desired storage directory path
STORAGE_PATH = Path(os.getenv("CRAWLEE_STORAGE_DIR", "/data/storage"))
DATA_DIRECTORY = STORAGE_PATH / "datasets"
MAX_DEPTH = int(os.getenv("MAX_DEPTH", 3))
MAX_PAGES = int(os.getenv("MAX_PAGES", 10))
RAG_API_KEY = os.getenv("RAG_API_KEY", None)
RAG_HOST = os.getenv("RAG_HOST", None)

CSS_TABLE_STYLE = """
        img {
            width: auto;
            max-width: 95%;
            height: auto;
        }
        table {
            border-collapse: collapse;
            margin-top: 50px;
            margin-bottom: 50px;
            width: 95%;
        }
        td, th {
            border: 1px solid #000000;
            text-align: left;
            padding: 8px;
        }
        """


class CrawlerApp:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not CrawlerApp._initialized:
            self.crawled_count = 0
            self.max_pages = MAX_PAGES
            self.max_depth = MAX_DEPTH
            self.crawler = BeautifulSoupCrawler(
                max_requests_per_crawl=self.max_pages,
                request_handler_timeout=timedelta(seconds=30),
            )
            self.pdf_urls = {}
            self.img_urls = {}
            self.meta_factory = RandomIDFactory()
            CrawlerApp._initialized = True

    def getFileMeta(self, url):
        metafacotry = self.meta_factory
        return metafacotry.registerfilemetadata(url)

    def transform_request(
        self, request_options: RequestOptions, **kwargs
    ) -> RequestOptions | RequestTransformAction:
        current_depth = kwargs.get(
            "depth", request_options.get("userData", {}).get("depth", 1)
        )

        if self.crawled_count >= self.max_pages:
            self.crawler.log.info(
                f"Skipping URL due to page limit ({self.crawled_count} > {self.max_pages}): {request_options['url']}"
            )
            return "skip"

        if current_depth > self.max_depth:
            self.crawler.log.info(
                f"Skipping URL due to depth limit ({current_depth} > {self.max_depth}): {request_options['url']}"
            )
            return "skip"

        if "/docs" in request_options["url"]:
            request_options["headers"] = HttpHeaders(
                {"Custom-Header": "value"})
            request_options.setdefault("userData", {})["depth"] = current_depth

        if "/blog" in request_options["url"]:
            request_options["label"] = "BLOG"
            request_options.setdefault("userData", {})["depth"] = current_depth

        if request_options["url"].endswith(".pdf"):
            self.crawler.log.info(
                f"Hanlding PDF later for batch download: {request_options['url']}"
            )
            return "skip"

        request_options.setdefault("userData", {})["depth"] = current_depth
        return request_options

    async def push_data_handler(
        self,
        context: BeautifulSoupCrawlingContext,
        dataset_id: str | None = None,
        metadata: FileMetadata | None = None,
    ):

        html_content = (
            context.soup.prettify() if context.soup else str(context.response.body)
        )

        root_shema_domain = get_root_scheme_domain_from_url(
            context.request.url)

        # Remove all existing style attributes
        for tag in context.soup.find_all(True):  # Find all tags
            if "style" in tag.attrs:
                del tag.attrs["style"]  # Remove the style attribute

        # Add the specified CSS style to the head section
        if context.soup.find("head"):
            new_style_tag = context.soup.new_tag("style", type="text/css")
            new_style_tag.string = CSS_TABLE_STYLE
            context.soup.find("head").append(new_style_tag)
        else:
            # If there is no head section, create one and add the style
            head_tag = context.soup.new_tag("head")
            new_style_tag = context.soup.new_tag("style", type="text/css")
            new_style_tag.string = CSS_TABLE_STYLE
            head_tag.append(new_style_tag)
            context.soup.append(head_tag)

        # Format html_href_tags URL
        html_href_tags = [
            tag
            for tag in context.soup.find_all("a")
            if tag.get("href")
            and tag.get("href").startswith("/")
            and tag["href"].split("#")[0].split("?")[0].lower().endswith(".html")
        ]

        for html_href_tag in html_href_tags:
            html_href_tag["href"] = root_shema_domain + html_href_tag["href"]

        # Format PDF URL
        pdf_tags = [
            tag
            for tag in context.soup.find_all("a")
            if tag.get("href") and tag["href"].lower().endswith(".pdf")
        ]

        if pdf_tags is not None and len(pdf_tags) > 0:
            pdf_base_dir = str(Path(DATA_DIRECTORY, "pdf"))
            # Modify the src attribute of each img tag
            pdf_urls = url_formater(
                context.request.url, pdf_tags, pdf_base_dir)
            for pdf_url in pdf_urls:
                if pdf_url not in self.pdf_urls:
                    self.pdf_urls[pdf_url] = pdf_url

        # Format Image URL
        img_tags = context.soup.find_all("img")
        if img_tags is not None and len(img_tags) > 0:
            img_base_dir = str(Path(DATA_DIRECTORY, "img"))
            # Modify the src attribute of each img tag
            img_urls = url_formater(
                context.request.url, img_tags, img_base_dir)
            for img_url in img_urls:
                if img_url not in self.pdf_urls:
                    self.img_urls[img_url] = img_url

        # Generate the modified HTML content
        html_content = str(context.soup)

        data = {
            "url": context.request.url,
            "html": html_content,
            "depth": context.request.user_data.get("depth", 1),
            "timestamp": asyncio.get_running_loop().time(),
        }

        await context.push_data(
            data, dataset_id=dataset_id, dataset_name=metadata["name"]
        )

    def setup_handlers(self):
        @self.crawler.router.default_handler
        async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
            if self.crawled_count >= self.max_pages:
                context.log.info("Reached maximum page limit, stopping crawl.")
                return

            meta_refresh_url = None
            if context.soup:
                meta_tag = context.soup.find(
                    "meta", attrs={"http-equiv": lambda x: x and x.lower() == "refresh"}
                )
                if meta_tag and "content" in meta_tag.attrs:
                    content = meta_tag["content"]
                    parts = content.split(";")
                    for part in parts:
                        if part.strip().startswith("URL="):
                            meta_refresh_url = part.split("=", 1)[1].strip()
                            break

            if meta_refresh_url:
                absolute_url = urljoin(
                    str(context.request.url), meta_refresh_url)
                context.log.info(
                    f"Meta refresh redirect detected. Add Request URL to crawler: {absolute_url}"
                )
                await self.crawler.add_requests([absolute_url])
            else:
                await context.enqueue_links(
                    transform_request_function=self.transform_request,
                    strategy="same-domain",
                    transform_request_function_kwargs={
                        "context": context,
                        "depth": context.request.user_data.get("depth", 1) + 1,
                    },
                )

            context.log.info(
                f"Processing {context.request.url} (Depth: {context.request.user_data.get('depth', 1)})."
            )

            metadata = self.getFileMeta(context.request.url)
            metadata["name"] = str("html" / Path(metadata["name"]))

            await self.push_data_handler(context=context, metadata=metadata)

            self.crawled_count += 1
            context.log.info(
                f"Crawled {self.crawled_count}/{self.max_pages} pages.")

        @self.crawler.on_skipped_request
        async def skipped_request_handler(url: str, reason: SkippedReason) -> None:
            if reason == "robots_txt":
                self.crawler.log.info(
                    f"Skipped {url} due to robots.txt rules.")

    async def run(self, start_urls: list[str]) -> None:
        self.setup_handlers()
        try:
            await self.crawler.run(start_urls)
            print(
                f"Crawl completed. Total pages crawled: {self.crawled_count}")
        except Exception as e:
            print(f"Crawl interrupted with error: {str(e)}")
        finally:
            print(f"Final count: {self.crawled_count} pages crawled")


def download_urls(
    app: CrawlerApp,
    processor: FileProcessor,
    urls: list[str],
    save_dir: Optional[str] = None,
):

    # Define a simple download funciton and progress callback

    def file_download(url: str, save_path: str) -> bool:
        """Download a single PDF file with synchronization protection."""

        def progress_callback(url, save_path, status, progress):
            filename = os.path.basename(save_path) if save_path else "unknown"
            app.crawler.log.debug(f"{status} on {filename}: {progress:.1f}%")

        try:

            # Check if file already exists to avoid re-downloading
            if os.path.exists(save_path):
                if progress_callback:
                    progress_callback(url, save_path, "already_exists", 0)
                return True

            # Stream the download to handle large files efficiently
            response = processor.session.get(
                url, stream=True, timeout=processor.timeout
            )
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
                            progress_callback(
                                url, save_path, "downloading", progress)

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

    # Download multiple PDFs
    app.crawler.log.info(
        f"Starting download of multiple files... size: {len(urls)}")
    start_time = time.time()

    successful = processor.batch_process_files(
        urls=urls, progress_function=file_download, save_base_path=save_dir
    )

    end_time = time.time()
    app.crawler.log.info(
        f"Downloaded {len(successful)} files in {end_time - start_time:.2f} seconds"
    )

    # Clean up
    processor.close()


def write_output_files(
    app: CrawlerApp, processor: FileProcessor, data_dir: str, target_tag: str
):
    source_urls = []
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".json"):
                source_urls.append(os.path.join(root, file))

    # Define a simple download funciton and progress callback
    # process_json_file(file_path=file, target_tag="html")

    def save_processed_json_file(data_dir: str, save_path: str = None) -> bool:
        """Download a single PDF file with synchronization protection."""

        def progress_callback(data_dir, save_path, status, progress):
            filename = os.path.basename(save_path) if save_path else data_dir
            app.crawler.log.debug(f"{status} on {filename}: {progress:.1f}%")

        try:
            result = process_json_file(
                file_path=data_dir, target_tag=target_tag)
            if progress_callback:
                progress_callback(data_dir, save_path, "completed", 100)

            return data_dir if result else None

        except requests.exceptions.RequestException as e:
            if progress_callback:
                progress_callback(data_dir, save_path, f"error: {str(e)}", 0)
            # Clean up partially downloaded file
            if os.path.exists(save_path):
                os.remove(save_path)
            return False
        except Exception as e:
            if progress_callback:
                progress_callback(data_dir, save_path, f"error: {str(e)}", 0)
            return False

    # Download multiple PDFs
    app.crawler.log.info(
        f"Starting download of multiple files... size: {len(source_urls)}"
    )
    start_time = time.time()

    successful = processor.batch_process_files(
        urls=source_urls, progress_function=save_processed_json_file
    )

    end_time = time.time()
    app.crawler.log.info(
        f"Downloaded {len(successful)} files in {end_time - start_time:.2f} seconds"
    )

    # Clean up
    processor.close()


def turncate_storage(folder_path):
    try:
        # shutil.rmtree will delete the directory and all its contents
        shutil.rmtree(folder_path)
        print(f"The folder at {folder_path} has been deleted successfully.")
    except Exception as e:
        print(f"An error occurred while deleting the folder: {e}")


async def main(base_url: list[str]) -> None:
    app = CrawlerApp()

    # Create processor instance
    processor = FileProcessor(max_workers=10)

    # # Truncate the Storage
    turncate_storage(STORAGE_PATH)

    start_urls = base_url
    await app.run(start_urls)

    pdf_urls = list(app.pdf_urls.keys())
    if len(pdf_urls) > 0:
        app.crawler.log.info(
            f"Processing URLs batch download for PDF, size: {len(pdf_urls)}"
        )
        save_dir_pdf = str(DATA_DIRECTORY / "pdf")
        download_urls(app, processor, save_dir=save_dir_pdf, urls=pdf_urls)

    img_urls = list(app.img_urls.keys())
    if len(img_urls) > 0:
        app.crawler.log.info(
            f"Processing URLs batch download for Images, size: {len(img_urls)}"
        )
        save_dir_img = str(DATA_DIRECTORY / "img")
        download_urls(app, processor, save_dir=save_dir_img, urls=img_urls)

    app.crawler.log.info("Extracting HTML files ... ")
    write_output_files(
        app, processor, data_dir=str(DATA_DIRECTORY / Path("html")), target_tag="html"
    )

    app.crawler.log.info(f"Data is saved at: {DATA_DIRECTORY}")
    app.crawler.log.info("######### Crawling Task Done ##########")


def send_to_rag(base_url: str, folder_name: str = None, workspace_name: str = None):
    # Replace 'YOUR_API_KEY' and 'path/to/file.pdf' with actual values
    target_pdfs = []

    target_html_pdfs = get_all_pdfs_from_directory(
        domain_url=base_url, storage_dir=str(DATA_DIRECTORY / "html")
    )

    target_content_pdfs = get_all_pdfs_from_directory(
        domain_url=base_url, storage_dir=str(DATA_DIRECTORY / "pdf")
    )

    target_tmp_pdfs = get_all_pdfs_in_temp_directory(
        storage_dir=str(DATA_DIRECTORY / "pdf-upload.tmp")
    )

    target_pdfs.extend(target_html_pdfs)
    target_pdfs.extend(target_content_pdfs)
    target_pdfs.extend(target_tmp_pdfs)

    total_size = len(target_pdfs)
    for idx, pdf_file_path in enumerate(target_pdfs):
        try:
            result = upload_document(
                api_key=RAG_API_KEY,
                host_url=RAG_HOST,
                file_path=pdf_file_path,
                fodler_name=folder_name,
                workspace_name=workspace_name,
            )
            if result:
                set_pdfs_upload_done([pdf_file_path], str(DATA_DIRECTORY))
                print(
                    f"Written to RAG Completed {idx}/{total_size} for: {pdf_file_path}"
                )
            else:
                print(f"Failed for file: {pdf_file_path}")
        except Exception as e:
            print(e)


if __name__ == "__main__":

    load_dotenv()

    base_url = ["https://www.wsd.gov.hk/en/home/index.html"]
    asyncio.run(main(base_url))
    send_to_rag(base_url="https://www.wsd.gov.hk",
                folder_name="WSD-Web-Domain-EN", workspace_name="WSD")

    base_url = ["https://www.wsd.gov.hk/tc/home/index.html"]
    asyncio.run(main(base_url))
    send_to_rag(base_url="https://www.wsd.gov.hk", folder_name="WSD-Web-Domain-TC", workspace_name="WSD")
