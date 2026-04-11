from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from playwright.sync_api import Browser, BrowserContext, Page, Playwright, TimeoutError, sync_playwright

from config import Settings, SourceConfig
from exceptions import PageLoadError, ScraperError
from utils import dataframe_from_rows, deduplicate_dataframe, save_csv_backup, with_retries


LOGGER = logging.getLogger(__name__)


class BaseScraper(ABC):
    def __init__(self, settings: Settings, source: SourceConfig) -> None:
        self.settings = settings
        self.source = source
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self):
        self.logger.info("Starting scrape for %s", self.source.source_label)
        with sync_playwright() as playwright:
            browser = None
            context = None
            page = None
            try:
                self.logger.info("%s browser launch starting", self.source.source_label)
                browser = with_retries(
                    operation_name=f"{self.source.name} browser launch",
                    action=lambda: self._launch_browser(playwright),
                )
                context = browser.new_context(viewport={"width": 1600, "height": 2400}, locale="en-US")
                page = context.new_page()
                page.set_default_timeout(45_000)
                self.logger.info("%s page created", self.source.source_label)
                rows = with_retries(
                    operation_name=f"{self.source.name} scrape",
                    action=lambda: self.scrape(page),
                )
                self._save_debug_artifacts(page)
            except Exception as error:  # noqa: BLE001
                self.logger.exception("%s scrape failed: %s", self.source.source_label, error)
                if page is not None:
                    self._save_debug_artifacts(page, suffix="_error")
                if isinstance(error, ScraperError):
                    raise
                raise ScraperError(f"{self.source.source_label} scrape failed") from error
            finally:
                if context is not None:
                    context.close()
                    self.logger.info("%s browser context closed", self.source.source_label)
                if browser is not None:
                    browser.close()
                    self.logger.info("%s browser closed", self.source.source_label)

        dataframe = deduplicate_dataframe(dataframe_from_rows(rows))
        save_csv_backup(dataframe, self.source.csv_output_path)
        self.logger.info("%s normalized rows: %s", self.source.source_label, len(dataframe))
        return dataframe

    def _launch_browser(self, playwright: Playwright) -> Browser:
        return playwright.chromium.launch(headless=self.settings.headless)

    def open_page(self, page: Page) -> None:
        self.logger.info("Opening %s", self.source.source_url)
        try:
            page.goto(self.source.source_url, wait_until="domcontentloaded", timeout=120_000)
            page.wait_for_timeout(5_000)
        except TimeoutError as error:
            self.logger.error("Page open timed out for %s: %s", self.source.source_url, error)
            raise PageLoadError(f"Timed out opening {self.source.source_url}") from error

    def scroll_page(self, page: Page) -> None:
        previous_height = -1
        for attempt in range(1, self.settings.max_scrolls + 1):
            current_height = page.evaluate("() => document.body.scrollHeight")
            page.mouse.wheel(0, max(current_height, 1200))
            page.wait_for_timeout(self.settings.scroll_pause_ms)
            updated_height = page.evaluate("() => document.body.scrollHeight")
            self.logger.info(
                "%s scroll %s/%s: %s -> %s",
                self.source.name,
                attempt,
                self.settings.max_scrolls,
                current_height,
                updated_height,
            )
            if updated_height == previous_height:
                break
            previous_height = updated_height

    def _save_debug_artifacts(self, page: Page, suffix: str = "") -> None:
        html_path = self.source.debug_html_path
        screenshot_path = self.source.debug_screenshot_path
        if suffix:
            html_path = html_path.with_name(f"{html_path.stem}{suffix}{html_path.suffix}")
            screenshot_path = screenshot_path.with_name(f"{screenshot_path.stem}{suffix}{screenshot_path.suffix}")

        html_path.write_text(page.content(), encoding="utf-8")
        page.screenshot(path=str(screenshot_path), full_page=True)
        self.logger.info("Saved debug artifacts to %s and %s", html_path, screenshot_path)

    @abstractmethod
    def scrape(self, page: Page) -> list[dict[str, str]]:
        raise NotImplementedError
