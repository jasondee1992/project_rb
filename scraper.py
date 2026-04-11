from __future__ import annotations

import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from playwright.sync_api import Browser, Page, Playwright, TimeoutError, sync_playwright

from config import DATAFRAME_COLUMNS, Settings


LOGGER = logging.getLogger(__name__)
LIVE_STATUS_PATTERN = re.compile(r"^(Q\d|HT|OT|Halftime|Final)$", re.IGNORECASE)
SCHEDULED_PATTERN = re.compile(r"(?P<month>\d{2})/(?P<day>\d{2})\s+\([^)]+\)\s+(?P<time>\d{2}:\d{2})")


def scrape_matches(settings: Settings) -> pd.DataFrame:
    settings.output_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        browser = _with_retries(
            operation_name="browser launch",
            action=lambda: _setup_browser(playwright, settings.headless),
        )
        try:
            page = browser.new_page(viewport={"width": 1600, "height": 2400})
            _with_retries(
                operation_name="page load",
                action=lambda: _load_page(page, settings.target_url),
            )
            _dismiss_terms_dialog(page)
            _scroll_page(page, settings.max_scrolls, settings.scroll_pause_ms)
            _save_debug_artifacts(page, settings.debug_html_path, settings.debug_screenshot_path)

            extracted = _with_retries(
                operation_name="DOM extraction",
                action=lambda: _extract_page_payload(page, settings.target_url),
            )
        finally:
            browser.close()

    rows = _normalize_rows(extracted, settings.target_url)
    dataframe = pd.DataFrame(rows)
    if dataframe.empty:
        dataframe = pd.DataFrame(columns=DATAFRAME_COLUMNS)
    else:
        dataframe = dataframe[DATAFRAME_COLUMNS]
        dataframe = dataframe.drop_duplicates().reset_index(drop=True)

    LOGGER.info("Normalized %s rows after cleanup", len(dataframe))
    return dataframe


def _setup_browser(playwright: Playwright, headless: bool) -> Browser:
    LOGGER.info("Launching Chromium with headless=%s", headless)
    return playwright.chromium.launch(headless=headless)


def _load_page(page: Page, url: str) -> None:
    LOGGER.info("Opening %s", url)
    page.goto(url, wait_until="domcontentloaded", timeout=120_000)
    page.wait_for_timeout(5_000)

    match_cards = page.locator(".s7k-match-box").count()
    current_market = _clean_text(page.locator(".s7k-function-right").inner_text()) if page.locator(".s7k-function-right").count() else ""
    LOGGER.info("Initial selector match: .s7k-match-box=%s, market=%s", match_cards, current_market or "unknown")


def _dismiss_terms_dialog(page: Page) -> None:
    dialog = page.locator(".term-check-dialog")
    if not dialog.count():
        return

    if not dialog.is_visible():
        return

    LOGGER.info("Terms dialog detected; applying best-effort dismissal")
    checkbox_wrappers = [
        ".terms-confirm-checkbox-age-wrapper",
        ".terms-confirm-checkbox-wrapper",
        ".terms-confirm-checkbox-agree-wrapper",
    ]

    for selector in checkbox_wrappers:
        locator = page.locator(selector)
        if locator.count():
            try:
                locator.first.click(timeout=2_000)
            except TimeoutError:
                LOGGER.debug("Checkbox click timed out for selector %s", selector)

    submit_button = page.locator(".term-check-dialog-submit button")
    try:
        if submit_button.count() and submit_button.is_enabled():
            submit_button.click(timeout=3_000)
            page.wait_for_timeout(1_000)
    except TimeoutError:
        LOGGER.debug("Submit button remained disabled")

    if dialog.is_visible():
        close_button = page.locator(".term-check-dialog .close_btn")
        try:
            if close_button.count():
                close_button.click(timeout=2_000)
                page.wait_for_timeout(500)
        except TimeoutError:
            LOGGER.debug("Close button click timed out")

    if dialog.is_visible():
        LOGGER.warning("Terms dialog still visible; removing overlay via DOM fallback")
        page.evaluate(
            """() => {
                for (const selector of ['#terms-check', '.term-check-dialog', '.full_page.term-check-dialog']) {
                    document.querySelectorAll(selector).forEach((node) => node.remove());
                }
                document.body.style.overflow = 'auto';
            }"""
        )
        page.wait_for_timeout(500)


def _scroll_page(page: Page, max_scrolls: int, pause_ms: int) -> None:
    LOGGER.info("Scrolling page up to %s times", max_scrolls)
    previous_height = 0

    for attempt in range(1, max_scrolls + 1):
        current_height = page.evaluate("() => document.body.scrollHeight")
        page.mouse.wheel(0, current_height)
        page.wait_for_timeout(pause_ms)
        updated_height = page.evaluate("() => document.body.scrollHeight")
        match_cards = page.locator(".s7k-match-box").count()
        LOGGER.info(
            "Scroll %s/%s: height=%s -> %s, cards=%s",
            attempt,
            max_scrolls,
            current_height,
            updated_height,
            match_cards,
        )

        if updated_height == previous_height:
            LOGGER.info("Scroll height stabilized after %s attempts", attempt)
            break
        previous_height = updated_height


def _save_debug_artifacts(page: Page, html_path: Path, screenshot_path: Path) -> None:
    LOGGER.info("Saving debug HTML to %s", html_path)
    html_path.write_text(page.content(), encoding="utf-8")
    LOGGER.info("Saving debug screenshot to %s", screenshot_path)
    page.screenshot(path=str(screenshot_path), full_page=True)


def _extract_page_payload(page: Page, source_url: str) -> dict[str, Any]:
    payload = page.evaluate(
        """(sourceUrl) => {
            const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
            const isVisible = (element) => {
                if (!element) {
                    return false;
                }
                const style = window.getComputedStyle(element);
                return style && style.display !== 'none' && style.visibility !== 'hidden';
            };

            const leagueNode = document.querySelector('.nav-item');
            const marketNode = document.querySelector('.s7k-function-right');
            const cards = Array.from(document.querySelectorAll('.s7k-match-box'))
                .filter((card) => isVisible(card))
                .map((card) => {
                    const teams = Array.from(card.querySelectorAll('.c1-name, .c2-name')).map((node) => normalize(node.textContent));
                    const detailLines = (card.querySelector('.match-bottom-detail')?.innerText || '')
                        .split(/\\n+/)
                        .map(normalize)
                        .filter(Boolean);

                    const visibleMarketContainer = Array.from(card.querySelectorAll('.right > div'))
                        .find((node) => isVisible(node) && node.querySelector('.selection-container'));

                    const selections = visibleMarketContainer
                        ? Array.from(visibleMarketContainer.querySelectorAll('.bet-selection')).map((selection, index) => ({
                            selection_index: index,
                            head: normalize(selection.querySelector('.head')?.innerText || ''),
                            odds: normalize(selection.querySelector('.odds')?.innerText || ''),
                        }))
                        : [];

                    return {
                        source_url: sourceUrl,
                        league_name: normalize(leagueNode?.childNodes?.[0]?.textContent || leagueNode?.textContent || ''),
                        market_type: normalize(marketNode?.textContent || ''),
                        home_team: teams[0] || '',
                        away_team: teams[1] || '',
                        detail_lines: detailLines,
                        selections,
                        raw_text: normalize(card.innerText),
                    };
                });

            return {
                league_name: normalize(leagueNode?.childNodes?.[0]?.textContent || leagueNode?.textContent || ''),
                market_type: normalize(marketNode?.textContent || ''),
                selector_matches: cards.length,
                cards,
            };
        }""",
        source_url,
    )

    LOGGER.info(
        "Matched selector .s7k-match-box=%s, league=%s, market=%s",
        payload.get("selector_matches", 0),
        payload.get("league_name") or "unknown",
        payload.get("market_type") or "unknown",
    )
    return payload


def _normalize_rows(payload: dict[str, Any], source_url: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    cards = payload.get("cards", [])

    for card in cards:
        match_date, match_time, match_status = _parse_match_detail(card.get("detail_lines", []))
        scraped_at = datetime.now().astimezone().isoformat(timespec="seconds")
        market_type = card.get("market_type") or payload.get("market_type") or "Unknown"
        league_name = card.get("league_name") or payload.get("league_name") or ""

        for selection in card.get("selections", []):
            selection_name, handicap_or_line = _parse_selection(
                market_type=market_type,
                selection_head=selection.get("head", ""),
                home_team=card.get("home_team", ""),
                away_team=card.get("away_team", ""),
                selection_index=selection.get("selection_index", 0),
            )

            rows.append(
                {
                    "scraped_at": scraped_at,
                    "source_url": source_url,
                    "category_name": "Basketball",
                    "league_name": league_name,
                    "sport_name": "Basketball",
                    "match_date": match_date,
                    "match_time": match_time,
                    "home_team": card.get("home_team", ""),
                    "away_team": card.get("away_team", ""),
                    "match_status": match_status,
                    "market_type": market_type,
                    "selection_name": selection_name,
                    "odds": selection.get("odds", ""),
                    "handicap_or_line": handicap_or_line,
                    "raw_text": card.get("raw_text", ""),
                }
            )

    dataframe = pd.DataFrame(rows, columns=DATAFRAME_COLUMNS)
    if dataframe.empty:
        return []

    nba_mask = dataframe["league_name"].str.contains("nba", case=False, na=False)
    filtered = dataframe[nba_mask].copy()
    LOGGER.info("Filtered %s NBA rows from %s normalized rows", len(filtered), len(dataframe))
    return filtered.fillna("").to_dict(orient="records")


def _parse_match_detail(detail_lines: list[str]) -> tuple[str, str, str]:
    if not detail_lines:
        return "", "", ""

    first_line = detail_lines[0]
    scheduled_match = SCHEDULED_PATTERN.search(first_line)
    if scheduled_match:
        now = datetime.now().astimezone()
        match_date = datetime(
            year=now.year,
            month=int(scheduled_match.group("month")),
            day=int(scheduled_match.group("day")),
        ).strftime("%Y-%m-%d")
        return match_date, scheduled_match.group("time"), "Scheduled"

    status = first_line
    match_time = ""
    if len(detail_lines) > 1 and re.fullmatch(r"\d{2}:\d{2}", detail_lines[1]):
        status = f"{first_line} {detail_lines[1]}"
    elif len(detail_lines) > 1 and LIVE_STATUS_PATTERN.match(first_line):
        status = f"{first_line} {detail_lines[1]}"

    match_date = datetime.now().astimezone().strftime("%Y-%m-%d")
    return match_date, match_time, status


def _parse_selection(
    market_type: str,
    selection_head: str,
    home_team: str,
    away_team: str,
    selection_index: int,
) -> tuple[str, str]:
    clean_head = _clean_text(selection_head)
    compact_head = clean_head.replace(" ", "")
    normalized_market = market_type.strip().upper()

    if normalized_market == "HDP":
        selection_name = home_team if selection_index == 0 else away_team
        return selection_name, clean_head

    if normalized_market in {"O/U", "OU", "TOTAL"}:
        if compact_head.upper().startswith("O"):
            return "Over", compact_head[1:]
        if compact_head.upper().startswith("U"):
            return "Under", compact_head[1:]
        return ("Over" if selection_index == 0 else "Under"), clean_head

    if normalized_market == "MATCH":
        selection_name = home_team if selection_index == 0 else away_team
        return selection_name, ""

    if compact_head.upper().startswith("O"):
        return "Over", compact_head[1:]
    if compact_head.upper().startswith("U"):
        return "Under", compact_head[1:]

    selection_name = home_team if selection_index == 0 else away_team
    return selection_name, clean_head


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _with_retries(operation_name: str, action, attempts: int = 3, delay_seconds: int = 3):
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except Exception as error:  # noqa: BLE001
            last_error = error
            LOGGER.warning("%s failed on attempt %s/%s: %s", operation_name, attempt, attempts, error)
            if attempt < attempts:
                time.sleep(delay_seconds)

    assert last_error is not None
    raise last_error
