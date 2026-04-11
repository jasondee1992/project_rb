from __future__ import annotations

import logging
import re
from datetime import datetime
from urllib.parse import urljoin

from playwright.sync_api import Page, TimeoutError

from scraper_base import BaseScraper
from exceptions import ModalHandlingError, SelectorNotFoundError
from utils import clean_text, current_timestamp, with_retries


LOGGER = logging.getLogger(__name__)

SCHEDULED_PATTERN = re.compile(r"(?P<month>\d{1,2})/(?P<day>\d{1,2}).*?(?P<time>\d{1,2}:\d{2})")
LIVE_TOKEN_PATTERN = re.compile(r"^(Q\d|HT|OT|HALFTIME|FINAL)$", re.IGNORECASE)
TARGET_MARKETS = {
    "Handicap (Incl. Overtime)",
    "Over / Under (Incl. Overtime)",
    "Winner (Incl. Overtime)",
}


class SportsPlusScraper(BaseScraper):
    def scrape(self, page: Page) -> list[dict[str, str]]:
        self.open_page(page)
        self._handle_terms_dialog(page)
        try:
            page.locator(".s7k-match-box").first.wait_for(state="visible", timeout=60_000)
        except TimeoutError as error:
            self.logger.error("Sports Plus selector lookup failed: .s7k-match-box")
            raise SelectorNotFoundError("Sports Plus match boxes were not visible") from error
        self.scroll_page(page)
        list_entries = self._extract_list_entries(page)
        self.logger.info("Sports Plus visible NBA list entries found: %s", len(list_entries))

        rows: list[dict[str, str]] = []
        for index, entry in enumerate(list_entries, start=1):
            home_team = clean_text(entry.get("home_team", ""))
            away_team = clean_text(entry.get("away_team", ""))
            href = clean_text(entry.get("href", ""))
            self.logger.info(
                "Opening Sports Plus match %s/%s: %s vs %s",
                index,
                len(list_entries),
                home_team,
                away_team,
            )

            try:
                self._open_match_detail(page, href)
                detail_rows = self._extract_detail_rows(page, entry)
                rows.extend(detail_rows)
                self.logger.info(
                    "Sports Plus detail rows captured for %s vs %s: %s",
                    home_team,
                    away_team,
                    len(detail_rows),
                )
            except Exception as error:  # noqa: BLE001
                self.logger.error(
                    "Sports Plus detail scrape failed for %s vs %s: %s",
                    home_team,
                    away_team,
                    error,
                )
            finally:
                self._return_to_match_list(page)

        nba_rows = [row for row in rows if "nba" in row["league_name"].lower()]
        LOGGER.info("Sports Plus extracted %s rows before final deduplication", len(nba_rows))
        return nba_rows

    def _extract_list_entries(self, page: Page) -> list[dict[str, str]]:
        payload = page.evaluate(
            """() => {
                const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                const isVisible = (element) => {
                    if (!element) {
                        return false;
                    }
                    const style = window.getComputedStyle(element);
                    const rect = element.getBoundingClientRect();
                    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
                };

                return Array.from(document.querySelectorAll('.s7k-match-box'))
                    .filter((card) => isVisible(card))
                    .map((card) => {
                        const teams = Array.from(card.querySelectorAll('.c1-name, .c2-name')).map((node) => normalize(node.textContent));
                        return {
                            href: normalize(card.getAttribute('href') || ''),
                            home_team: teams[0] || '',
                            away_team: teams[1] || '',
                            detail_text: normalize(card.querySelector('.match-bottom-detail')?.innerText || ''),
                            raw_text: normalize(card.innerText || ''),
                        };
                    });
            }"""
        )
        entries = [entry for entry in payload if clean_text(entry.get("home_team", "")) and clean_text(entry.get("away_team", ""))]
        if not entries:
            raise SelectorNotFoundError("Sports Plus list entries could not be extracted")
        return entries

    def _open_match_detail(self, page: Page, href: str) -> None:
        if not href:
            raise RuntimeError("Sports Plus match href was missing")
        match_url = urljoin(self.source.source_url, href)
        def click_and_wait() -> None:
            page.locator(f'.s7k-match-box[href="{href}"]').first.click(timeout=10_000)
            page.wait_for_url(f"**{href}", timeout=60_000)
            page.locator(".s7k-smv-marketlines").first.wait_for(state="visible", timeout=60_000)
            page.wait_for_timeout(2_000)

        with_retries(
            operation_name=f"Sports Plus match click {href}",
            action=click_and_wait,
            attempts=2,
            delay_seconds=1.0,
        )
        self.logger.info("Sports Plus detail page loaded: %s", page.url)
        if page.url != match_url:
            self.logger.debug("Expected detail URL %s, actual %s", match_url, page.url)

    def _return_to_match_list(self, page: Page) -> None:
        if "/sbk/m/" not in page.url:
            return
        try:
            page.go_back(wait_until="domcontentloaded", timeout=120_000)
            page.locator(".s7k-match-box").first.wait_for(state="visible", timeout=60_000)
            page.wait_for_timeout(1_000)
        except Exception as error:  # noqa: BLE001
            self.logger.warning("Navigation back failed; recovering via list URL: %s", error)
            page.goto(self.source.source_url, wait_until="domcontentloaded", timeout=120_000)
            self._handle_terms_dialog(page)
            page.locator(".s7k-match-box").first.wait_for(state="visible", timeout=60_000)
            page.wait_for_timeout(1_000)

    def _extract_detail_rows(self, page: Page, entry: dict[str, str]) -> list[dict[str, str]]:
        payload = page.evaluate(
            """() => {
                const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                const isVisible = (element) => {
                    if (!element) {
                        return false;
                    }
                    const style = window.getComputedStyle(element);
                    const rect = element.getBoundingClientRect();
                    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
                };

                const navText = normalize(document.querySelector('.nav-item')?.innerText || '');
                const topSection = document.querySelector('.s7k-smv-top-section');
                const timeText = normalize(topSection?.querySelector('.section-bar > div:first-child span')?.innerText || '');
                const statusText = normalize(topSection?.querySelector('.center')?.innerText || '');

                const markets = Array.from(document.querySelectorAll('.marketline-box'))
                    .filter((market) => isVisible(market))
                    .map((market) => ({
                        market_type: normalize(market.querySelector('.market-title-content')?.innerText || '').replace(/\\s*[\\u25be\\u25b4].*$/, ''),
                        column_headers: Array.from(market.querySelectorAll('.bc-title')).map((node) => normalize(node.innerText)),
                        selections: Array.from(market.querySelectorAll('.bet-selection'))
                            .filter((selection) => isVisible(selection))
                            .map((selection, index) => ({
                                selection_index: index,
                                head: normalize(selection.querySelector('.head')?.innerText || ''),
                                odds: normalize(selection.querySelector('.odds')?.innerText || ''),
                            })),
                        raw_text: normalize(market.innerText || ''),
                    }));

                return {
                    nav_text: navText,
                    time_text: timeText,
                    status_text: statusText,
                    markets,
                };
            }"""
        )

        home_team = clean_text(entry.get("home_team", ""))
        away_team = clean_text(entry.get("away_team", ""))
        nav_text = clean_text(payload.get("nav_text", ""))
        if nav_text and "v" in nav_text.lower():
            parsed_home, parsed_away = self._parse_nav_matchup(nav_text)
            home_team = parsed_home or home_team
            away_team = parsed_away or away_team

        match_date, match_time, status_from_list, category_name = self._parse_match_detail(entry.get("detail_text", ""))
        detail_date, detail_time = self._parse_detail_datetime(clean_text(payload.get("time_text", "")))
        match_status = clean_text(payload.get("status_text", "")) or status_from_list
        if detail_date:
            match_date = detail_date
        if detail_time:
            match_time = detail_time
        if match_status and "not started" in match_status.lower():
            category_name = "TODAY"
            match_status = "Scheduled"

        scraped_at = current_timestamp()
        rows: list[dict[str, str]] = []

        for market in payload.get("markets", []):
            market_type = clean_text(market.get("market_type", ""))
            if market_type not in TARGET_MARKETS:
                continue

            column_headers = [clean_text(value) for value in market.get("column_headers", [])]
            for selection in market.get("selections", []):
                selection_name, handicap_or_line = self._parse_detail_selection(
                    market_type=market_type,
                    selection_head=selection.get("head", ""),
                    selection_index=int(selection.get("selection_index", 0)),
                    column_headers=column_headers,
                    home_team=home_team,
                    away_team=away_team,
                )
                rows.append(
                    {
                        "source_label": self.source.source_label,
                        "source_site": self.source.source_site,
                        "source_url": self.source.source_url,
                        "scraped_at": scraped_at,
                        "category_name": category_name,
                        "league_name": "NBA",
                        "sport_name": "Basketball",
                        "match_date": match_date,
                        "match_time": match_time,
                        "home_team": home_team,
                        "away_team": away_team,
                        "match_status": match_status,
                        "market_type": market_type,
                        "selection_name": selection_name,
                        "odds": clean_text(selection.get("odds", "")),
                        "handicap_or_line": handicap_or_line,
                        "raw_text": clean_text(market.get("raw_text", "")),
                    }
                )

        return rows

    def _handle_terms_dialog(self, page: Page) -> None:
        self.logger.info("Checking Sports Plus consent modal state")
        try:
            page.wait_for_function(
                """() => {
                    const dialog = document.querySelector('.term-check-dialog');
                    const cards = document.querySelectorAll('.s7k-match-box').length;
                    if (!dialog) {
                        return cards > 0;
                    }
                    const style = window.getComputedStyle(dialog);
                    const visible = style.display !== 'none' && style.visibility !== 'hidden';
                    return visible || cards > 0;
                }""",
                timeout=15_000,
            )
        except TimeoutError:
            self.logger.info("Consent modal did not appear within the wait window")

        dialog = page.locator(".term-check-dialog").first
        if not dialog.count() or not dialog.is_visible():
            self.logger.info("Sports Plus consent modal not present; continuing normally")
            return

        self.logger.info("Sports Plus consent modal detected")
        self._save_debug_artifacts(page, suffix="_before_consent")

        for selector in (
            ".terms-confirm-checkbox-age-wrapper",
            ".terms-confirm-checkbox-wrapper",
            ".terms-confirm-checkbox-agree-wrapper",
        ):
            locator = page.locator(selector).filter(has=page.locator("input"))
            if locator.count() and locator.first.is_visible():
                try:
                    locator.first.click(timeout=2_000)
                    self.logger.info("Clicked consent control %s", selector)
                except TimeoutError:
                    self.logger.warning("Timed out clicking consent control %s", selector)

        submit_button = page.locator(".term-check-dialog-submit button").first
        if not submit_button.count():
            raise RuntimeError("Sports Plus consent modal was shown but Submit button was not found")

        try:
            page.wait_for_function(
                """() => {
                    const button = document.querySelector('.term-check-dialog-submit button');
                    return !!button && !button.disabled;
                }""",
                timeout=3_000,
            )
        except TimeoutError:
            self.logger.info("Consent Submit button still disabled after UI clicks; applying checkbox event fallback")
            page.evaluate(
                """() => {
                    const inputs = Array.from(document.querySelectorAll('.term-check-dialog input[type="checkbox"]'));
                    for (const input of inputs) {
                        if (!input.checked) {
                            input.checked = true;
                            input.dispatchEvent(new Event('input', { bubbles: true }));
                            input.dispatchEvent(new Event('change', { bubbles: true }));
                        }
                    }
                }"""
            )
            try:
                page.wait_for_function(
                    """() => {
                        const button = document.querySelector('.term-check-dialog-submit button');
                        return !!button && !button.disabled;
                    }""",
                    timeout=10_000,
                )
            except TimeoutError as error:
                self.logger.error("Sports Plus modal interaction failed: Submit stayed disabled")
                raise ModalHandlingError("Sports Plus consent Submit button did not become enabled") from error

        submit_button.click(timeout=5_000)
        self.logger.info("Clicked Sports Plus consent Submit button")

        try:
            dialog.wait_for(state="hidden", timeout=15_000)
        except TimeoutError as error:
            self.logger.error("Sports Plus modal interaction failed: dialog did not disappear")
            raise ModalHandlingError("Sports Plus consent modal did not disappear after Submit") from error

        page.wait_for_timeout(1_000)
        self._save_debug_artifacts(page, suffix="_after_consent")
        self.logger.info("Sports Plus consent modal handled successfully")

    def _parse_match_detail(self, detail_text: str) -> tuple[str, str, str, str]:
        cleaned = clean_text(detail_text)
        if not cleaned:
            return "", "", "", ""

        scheduled_match = SCHEDULED_PATTERN.search(cleaned)
        if scheduled_match:
            now = datetime.now().astimezone()
            try:
                match_date = datetime(
                    year=now.year,
                    month=int(scheduled_match.group("month")),
                    day=int(scheduled_match.group("day")),
                    tzinfo=now.tzinfo,
                ).strftime("%Y-%m-%d")
            except ValueError:
                match_date = ""
            return match_date, scheduled_match.group("time"), "Scheduled", "TODAY"

        tokens = cleaned.split(" ")
        status = cleaned
        category_name = ""
        if tokens and LIVE_TOKEN_PATTERN.match(tokens[0]):
            category_name = "LIVE"
        return datetime.now().astimezone().strftime("%Y-%m-%d"), "", status, category_name

    def _parse_selection(
        self,
        market_type: str,
        selection_head: str,
        selection_index: int,
        home_team: str,
        away_team: str,
    ) -> tuple[str, str]:
        cleaned_head = clean_text(selection_head)
        compact_head = cleaned_head.replace(" ", "")
        market_upper = market_type.strip().upper()

        if market_upper == "HDP":
            return (home_team if selection_index == 0 else away_team), cleaned_head

        if market_upper in {"O/U", "OU", "TOTAL"}:
            if compact_head.upper().startswith("O"):
                return "Over", compact_head[1:]
            if compact_head.upper().startswith("U"):
                return "Under", compact_head[1:]
            return ("Over" if selection_index == 0 else "Under"), cleaned_head

        if market_upper == "MATCH":
            return (home_team if selection_index == 0 else away_team), ""

        if cleaned_head:
            return cleaned_head, cleaned_head

        return (home_team if selection_index == 0 else away_team), ""

    def _parse_detail_datetime(self, detail_time_text: str) -> tuple[str, str]:
        cleaned = clean_text(detail_time_text)
        if not cleaned:
            return "", ""
        scheduled_match = SCHEDULED_PATTERN.search(cleaned)
        if not scheduled_match:
            return "", ""
        now = datetime.now().astimezone()
        try:
            match_date = datetime(
                year=now.year,
                month=int(scheduled_match.group("month")),
                day=int(scheduled_match.group("day")),
                tzinfo=now.tzinfo,
            ).strftime("%Y-%m-%d")
        except ValueError:
            match_date = ""
        return match_date, scheduled_match.group("time")

    def _parse_nav_matchup(self, nav_text: str) -> tuple[str, str]:
        cleaned = clean_text(nav_text)
        parts = re.split(r"\s+v\s+", cleaned, maxsplit=1, flags=re.IGNORECASE)
        if len(parts) == 2:
            return clean_text(parts[0]), clean_text(parts[1])
        return "", ""

    def _parse_detail_selection(
        self,
        market_type: str,
        selection_head: str,
        selection_index: int,
        column_headers: list[str],
        home_team: str,
        away_team: str,
    ) -> tuple[str, str]:
        cleaned_head = clean_text(selection_head)
        compact_head = cleaned_head.replace(" ", "")

        if market_type == "Handicap (Incl. Overtime)":
            selection_name = column_headers[selection_index] if selection_index < len(column_headers) else (home_team if selection_index == 0 else away_team)
            return selection_name, cleaned_head

        if market_type == "Over / Under (Incl. Overtime)":
            selection_name = column_headers[selection_index] if selection_index < len(column_headers) else ("Over" if selection_index == 0 else "Under")
            return selection_name, cleaned_head

        if market_type == "Winner (Incl. Overtime)":
            if compact_head == "1":
                return home_team, ""
            if compact_head == "2":
                return away_team, ""
            selection_name = column_headers[selection_index] if selection_index < len(column_headers) else cleaned_head
            return selection_name, ""

        return cleaned_head, cleaned_head
