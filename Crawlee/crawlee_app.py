from __future__ import annotations
import os
import asyncio
from datetime import timedelta
from pathlib import Path
import shutil
import time
from typing import Dict, Any
from urllib.parse import urljoin
from util.random_id_factory import RandomIDFactory
from processor.pdf_processor import PDFProcessor
from crawlee import (
    Glob,
    HttpHeaders,
    RequestOptions,
    RequestTransformAction,
    SkippedReason,
)
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


# Set the desired storage directory path
STORAGE_PATH = Path(f"{os.getcwd()}/data/storage").resolve()
DATA_DIRECTORY = STORAGE_PATH / "datasets" / "default"
OBJ_DIRECTORY = STORAGE_PATH / "object"
os.environ["CRAWLEE_STORAGE_DIR"] = str(STORAGE_PATH)

# Global Variable
MAX_DEPTH = 3
MAX_PAGES = 100


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
            self.urls = {}
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
                f"Hanlding PDF document for batch download: {request_options['url']}"
            )
            url = request_options["url"]
            if url not in self.urls:
                self.urls[url] = url

            return "skip"

        request_options.setdefault("userData", {})["depth"] = current_depth
        return request_options

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

            data = {
                "url": context.request.url,
                "html": (
                    context.soup.prettify()
                    if context.soup
                    else str(context.response.body)
                ),
                "depth": context.request.user_data.get("depth", 1),
                "timestamp": asyncio.get_running_loop().time(),
            }

            metas = self.getFileMeta(context.request.url)

            await context.push_data(data, dataset_name=metas["name"])

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


async def main() -> None:
    app = CrawlerApp()
    start_urls = ["https://www.wsd.gov.hk/"]
    await app.run(start_urls)

    urls = list(app.urls.keys())
    if len(urls) > 0:
        app.crawler.log.info(f"Processing URLs batch download, size: {len(urls)}")
        download_urls(app, urls)


def download_urls(app: CrawlerApp, urls: list[str]):
    # Create processor instance
    processor = PDFProcessor(max_workers=10)
    save_dir = str(OBJ_DIRECTORY / "pdf")

    # Define a simple progress callback
    def progress_callback(url, save_path, status, progress):
        filename = os.path.basename(save_path) if save_path else "unknown"
        app.crawler.log.debug(f"{filename}: {status} {progress:.1f}%")

    # Download multiple PDFs
    app.crawler.log.info(f"Starting download of multiple PDFs... size: {len(urls)}")
    start_time = time.time()

    successful = processor.download_multiple_pdfs(
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


if __name__ == "__main__":
    turncate_storage(STORAGE_PATH)
    asyncio.run(main())
