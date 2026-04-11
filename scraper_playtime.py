from __future__ import annotations

import logging
from datetime import datetime

from playwright.sync_api import Locator, Page, TimeoutError

from scraper_base import BaseScraper
from exceptions import SelectorNotFoundError
from utils import (
    clean_text,
    current_timestamp,
    parse_match_datetime_from_text,
    parse_match_datetime_from_timestamp,
    with_retries,
)


LOGGER = logging.getLogger(__name__)


class PlaytimeScraper(BaseScraper):
    def scrape(self, page: Page) -> list[dict[str, str]]:
        self.open_page(page)
        self._select_basketball_today_matches(page)
        try:
            page.wait_for_function(
                """() => Array.from(document.querySelectorAll('.odds-container .league span'))
                    .some((node) => (node.textContent || '').replace(/\\s+/g, ' ').trim() === 'NBA')""",
                timeout=10_000,
            )
        except TimeoutError:
            self.logger.info("Exact NBA league block did not appear before scroll; continuing with best-effort extraction")
        self.scroll_page(page)
        self.logger.info("Playtime NBA detection starting")

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

                const parseLine = (anchor) => {
                    const anchorId = normalize(anchor?.id || '');
                    const idLine = anchorId ? anchorId.split('|').slice(-1)[0] : '';
                    const prefixText = normalize(anchor?.querySelector('.hdp p')?.innerText || '');
                    const prefixNumeric = prefixText.match(/-?\\d+(?:\\.\\d+)?/);
                    return prefixNumeric ? prefixNumeric[0] : idLine;
                };

                const buildLockedSelections = (type, homeTeam, awayTeam) => {
                    if (type.includes('ML') || type.includes('HDP')) {
                        return [
                            { market_type: type, selection_name: homeTeam, odds: '', handicap_or_line: '', event_timestamp_ms: '' },
                            { market_type: type, selection_name: awayTeam, odds: '', handicap_or_line: '', event_timestamp_ms: '' },
                        ];
                    }
                    if (type.includes('OU')) {
                        return [
                            { market_type: type, selection_name: 'Over', odds: '', handicap_or_line: '', event_timestamp_ms: '' },
                            { market_type: type, selection_name: 'Under', odds: '', handicap_or_line: '', event_timestamp_ms: '' },
                        ];
                    }
                    return [];
                };

                const parseMarketSelections = (cell, type, homeTeam, awayTeam) => {
                    if (!cell || !isVisible(cell)) {
                        return [];
                    }

                    const anchors = Array.from(cell.querySelectorAll('a.odds')).filter(isVisible);
                    const lockCount = cell.querySelectorAll('.icon-lock').length;
                    if (!anchors.length) {
                        return lockCount >= 2 ? buildLockedSelections(type, homeTeam, awayTeam) : [];
                    }

                    return anchors.map((anchor, index) => {
                        const prefixText = normalize(anchor.querySelector('.hdp p')?.innerText || '');
                        let selectionName = '';
                        let handicapOrLine = '';

                        if (type.includes('ML')) {
                            selectionName = index === 0 ? homeTeam : awayTeam;
                        } else if (type.includes('HDP')) {
                            selectionName = index === 0 ? homeTeam : awayTeam;
                            handicapOrLine = parseLine(anchor);
                        } else if (type.includes('OU')) {
                            handicapOrLine = parseLine(anchor);
                            if (prefixText.toLowerCase().startsWith('u')) {
                                selectionName = 'Under';
                            } else if (prefixText.toLowerCase().startsWith('o')) {
                                selectionName = 'Over';
                            } else {
                                selectionName = index === 0 ? 'Over' : 'Under';
                            }
                        }

                        return {
                            market_type: type,
                            selection_name: selectionName,
                            odds: normalize(anchor.querySelector('span:last-child')?.innerText || anchor.innerText || ''),
                            handicap_or_line: handicapOrLine,
                            event_timestamp_ms: normalize(anchor.getAttribute('data-date') || ''),
                        };
                    });
                };

                const leagues = Array.from(document.querySelectorAll('.odds-container .league'))
                    .filter((leagueNode) =>
                        Array.from(leagueNode.querySelectorAll('span'))
                            .some((node) => normalize(node.textContent) === 'NBA')
                    )
                    .map((leagueNode) => {
                        const wrapper = leagueNode.nextElementSibling;
                        const tables = wrapper ? Array.from(wrapper.querySelectorAll('table.events')) : [];
                        const events = [];

                        for (const table of tables) {
                            const eventRows = Array.from(table.querySelectorAll('tbody tr'))
                                .filter((row) => isVisible(row) && !row.classList.contains('more-bets'));
                            if (!eventRows.length) {
                                continue;
                            }

                            const nameCell = table.querySelector('.enhanced-event-name[data-home-team]');
                            if (!nameCell) {
                                continue;
                            }

                            const homeTeam = normalize(nameCell.getAttribute('data-home-team') || '');
                            const awayTeam = normalize(nameCell.getAttribute('data-away-team') || '');
                            const sportName = normalize(nameCell.getAttribute('data-sport-name') || 'Basketball');
                            const leagueName = normalize(nameCell.getAttribute('data-league-name') || 'NBA');
                            const timeCell = table.querySelector('.col-time.main-time');
                            const timeText = normalize(timeCell?.innerText || '');
                            const liveScore = normalize(timeCell?.querySelector('.live-score')?.innerText || '');
                            const liveInfo = normalize(timeCell?.querySelector('.liveInfo')?.innerText || '');
                            const rawText = normalize(eventRows.map((row) => row.innerText).join(' | '));

                            const markets = [];
                            for (const row of eventRows) {
                                markets.push(
                                    ...parseMarketSelections(row.querySelector('.main-1x2[data-period="0"]'), 'Game ML', homeTeam, awayTeam),
                                    ...parseMarketSelections(row.querySelector('.main-hdp[data-period="0"]'), 'Game HDP', homeTeam, awayTeam),
                                    ...parseMarketSelections(row.querySelector('.main-ou[data-period="0"]'), 'Game OU', homeTeam, awayTeam),
                                    ...parseMarketSelections(row.querySelector('.main-1x2[data-period="1"]'), '1st Half ML', homeTeam, awayTeam),
                                    ...parseMarketSelections(row.querySelector('.main-hdp[data-period="1"]'), '1st Half HDP', homeTeam, awayTeam),
                                    ...parseMarketSelections(row.querySelector('.main-ou[data-period="1"]'), '1st Half OU', homeTeam, awayTeam),
                                );
                            }

                            events.push({
                                category_name: 'TODAY',
                                league_name: leagueName,
                                sport_name: sportName,
                                home_team: homeTeam,
                                away_team: awayTeam,
                                time_text: timeText,
                                live_score: liveScore,
                                live_info: liveInfo,
                                raw_text: rawText,
                                markets,
                            });
                        }

                        return {
                            league_name: normalize(leagueNode.textContent),
                            events,
                        };
                    });

                return { leagues };
            }"""
        )

        rows: list[dict[str, str]] = []
        scraped_at = current_timestamp()

        for league in payload.get("leagues", []):
            for event in league.get("events", []):
                match_date, match_time = self._resolve_match_datetime(event)
                match_status = self._build_match_status(event)

                for market in event.get("markets", []):
                    rows.append(
                        {
                            "source_label": self.source.source_label,
                            "source_site": self.source.source_site,
                            "source_url": self.source.source_url,
                            "scraped_at": scraped_at,
                            "category_name": clean_text(event.get("category_name", "TODAY")),
                            "league_name": clean_text(event.get("league_name", "NBA")),
                            "sport_name": clean_text(event.get("sport_name", "Basketball")),
                            "match_date": match_date,
                            "match_time": match_time,
                            "home_team": clean_text(event.get("home_team", "")),
                            "away_team": clean_text(event.get("away_team", "")),
                            "match_status": match_status,
                            "market_type": clean_text(market.get("market_type", "")),
                            "selection_name": clean_text(market.get("selection_name", "")),
                            "odds": clean_text(market.get("odds", "")),
                            "handicap_or_line": clean_text(market.get("handicap_or_line", "")),
                            "raw_text": clean_text(event.get("raw_text", "")),
                        }
                    )

        nba_rows = [row for row in rows if "nba" in row["league_name"].lower()]
        if not nba_rows:
            self.logger.warning("Playtime NBA section was not available; emitting placeholder row")
            return [self._build_placeholder_row(scraped_at)]

        LOGGER.info("Playtime extracted %s rows before final deduplication", len(nba_rows))
        return nba_rows

    def _select_basketball_today_matches(self, page: Page) -> None:
        self.logger.info("Playtime selector lookup for Basketball > Today > Matches")
        basketball_component = page.locator(
            ".SportNoLiveTabContentComponent .SportMenuItemComponent",
            has_text="Basketball",
        ).filter(has=page.locator(".SportMenuItemTabComponent"))
        if not basketball_component.count():
            raise SelectorNotFoundError("Playtime Basketball component was not found")

        basketball_info = basketball_component.first.locator(".SportMenuItemInfoComponent").first
        class_name = clean_text(basketball_info.get_attribute("class") or "")
        if "selected" not in class_name.lower():
            self._click_with_retry(basketball_info, "Playtime Basketball selector")
            page.wait_for_timeout(1_000)

        self._click_first_if_present(basketball_component.first.locator(".market-tab", has_text="Today"), "Playtime Today tab")
        self._click_first_if_present(basketball_component.first.locator(".market-name", has_text="Matches"), "Playtime Matches market")
        page.wait_for_timeout(2_000)

    def _click_first_if_present(self, locator: Locator, label: str) -> None:
        if locator.count():
            self._click_with_retry(locator.first, label)

    def _click_with_retry(self, locator: Locator, label: str) -> None:
        with_retries(
            operation_name=label,
            action=lambda: locator.click(timeout=5_000),
            attempts=2,
            delay_seconds=1.0,
        )

    def _resolve_match_datetime(self, event: dict[str, str]) -> tuple[str, str]:
        for market in event.get("markets", []):
            match_date, match_time = parse_match_datetime_from_timestamp(market.get("event_timestamp_ms"))
            if match_date or match_time:
                return match_date, match_time
        match_date, match_time = parse_match_datetime_from_text(event.get("time_text", ""))
        if match_date or match_time:
            return match_date, match_time

        if clean_text(event.get("live_info", "")) or clean_text(event.get("time_text", "")):
            return datetime.now().astimezone().strftime("%Y-%m-%d"), ""
        return "", ""

    def _build_match_status(self, event: dict[str, str]) -> str:
        live_info = clean_text(event.get("live_info", ""))
        live_score = clean_text(event.get("live_score", ""))
        if live_info and live_score:
            return f"{live_info} ({live_score})"
        if live_info:
            return live_info
        if live_score:
            return live_score

        time_text = clean_text(event.get("time_text", ""))
        if time_text:
            return "Scheduled"
        return ""

    def _build_placeholder_row(self, scraped_at: str) -> dict[str, str]:
        return {
            "source_label": self.source.source_label,
            "source_site": self.source.source_site,
            "source_url": self.source.source_url,
            "scraped_at": scraped_at,
            "category_name": "Basketball",
            "league_name": "NBA",
            "sport_name": "Basketball",
            "match_date": "",
            "match_time": "",
            "home_team": "",
            "away_team": "",
            "match_status": "none as of this moment",
            "market_type": "",
            "selection_name": "",
            "odds": "",
            "handicap_or_line": "",
            "raw_text": "NBA not available as of this moment",
        }
