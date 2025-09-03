from __future__ import annotations
import os
import json
import asyncio
from dotenv import load_dotenv
from datetime import timedelta
from pathlib import Path
import shutil
import time
from typing import Dict, Any
from urllib.parse import urljoin
from util.random_id_factory import RandomIDFactory, FileMetadata
from util.htmlfile_writer import read_json_files_with_buffer
from processor.file_processor import (
    FileProcessor,
    get_root_domain_from_url,
    get_save_path_from_url,
    url_formater,
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


# Set the desired storage directory path
STORAGE_PATH = Path(os.getenv("CRAWLEE_STORAGE_DIR", "/data/storage"))
DATA_DIRECTORY = STORAGE_PATH / "datasets"
MAX_DEPTH = int(os.getenv("MAX_DEPTH", 3))
MAX_PAGES = int(os.getenv("MAX_PAGES", 10))


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
            request_options["headers"] = HttpHeaders({"Custom-Header": "value"})
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

        ## Format PDF URL
        pdf_tags = [
            tag
            for tag in context.soup.find_all("a")
            if tag.get("href") and tag["href"].lower().endswith(".pdf")
        ]

        if pdf_tags is not None and len(pdf_tags) > 0:
            pdf_base_dir = str(Path(DATA_DIRECTORY, "pdf"))
            # Modify the src attribute of each img tag
            pdf_urls = url_formater(context.request.url, pdf_tags, pdf_base_dir)
            for pdf_url in pdf_urls:
                if pdf_url not in self.pdf_urls:
                    self.pdf_urls[pdf_url] = pdf_url

        ## Format Image URL
        img_tags = context.soup.find_all("img")
        if img_tags is not None and len(img_tags) > 0:
            img_base_dir = str(Path(DATA_DIRECTORY, "img"))
            # Modify the src attribute of each img tag
            img_urls = url_formater(context.request.url, img_tags, img_base_dir)
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
                absolute_url = urljoin(str(context.request.url), meta_refresh_url)
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
            context.log.info(f"Crawled {self.crawled_count}/{self.max_pages} pages.")

        @self.crawler.on_skipped_request
        async def skipped_request_handler(url: str, reason: SkippedReason) -> None:
            if reason == "robots_txt":
                self.crawler.log.info(f"Skipped {url} due to robots.txt rules.")

    async def run(self, start_urls: list[str]) -> None:
        self.setup_handlers()
        try:
            await self.crawler.run(start_urls)
            print(f"Crawl completed. Total pages crawled: {self.crawled_count}")
        except Exception as e:
            print(f"Crawl interrupted with error: {str(e)}")
        finally:
            print(f"Final count: {self.crawled_count} pages crawled")


def download_urls(app: CrawlerApp, processor: FileProcessor, save_dir, urls: list[str]):

    # Define a simple progress callback
    def progress_callback(url, save_path, status, progress):
        filename = os.path.basename(save_path) if save_path else "unknown"
        app.crawler.log.debug(f"{filename}: {status} {progress:.1f}%")

    # Download multiple PDFs
    app.crawler.log.info(f"Starting download of multiple files... size: {len(urls)}")
    start_time = time.time()

    successful = processor.download_files(
        urls, save_dir, progress_callback=progress_callback
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


def write_html(data_dir, target_tag):
    return read_json_files_with_buffer(DATA_DIRECTORY, target_tag="html")


async def main() -> None:
    turncate_storage(STORAGE_PATH)

    app = CrawlerApp()

    # Create processor instance
    processor = FileProcessor(max_workers=10)

    start_urls = ["https://www.wsd.gov.hk/"]
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

    write_html(data_dir=DATA_DIRECTORY, target_tag="html")


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(main())
