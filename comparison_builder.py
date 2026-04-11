from __future__ import annotations

from dataclasses import dataclass
from itertools import zip_longest

import pandas as pd

from exceptions import ComparisonError
from utils import clean_text, current_timestamp


COMPARISON_COLUMNS = [
    "scraped_at",
    "match_date",
    "match_time",
    "home_team",
    "away_team",
    "market_type",
    "selection_name",
    "handicap_or_line",
    "sportsplus_odds",
    "playtime_odds",
    "stake_amount",
    "sportsplus_return",
    "playtime_return",
    "comparison_status",
]

STAKE_AMOUNT = 1000.0

SPORTSPLUS_MARKET_MAP = {
    "Handicap (Incl. Overtime)": "Handicap",
    "Over / Under (Incl. Overtime)": "Over / Under",
    "Winner (Incl. Overtime)": "Winner",
}

PLAYTIME_MARKET_MAP = {
    "Game HDP": "Handicap",
    "Game OU": "Over / Under",
    "Game ML": "Winner",
}


@dataclass(frozen=True)
class ComparableEntry:
    source_site: str
    scraped_at: str
    match_date: str
    match_time: str
    home_team: str
    away_team: str
    market_type: str
    selection_name: str
    handicap_or_line: str
    odds: str

    @property
    def base_key(self) -> tuple[str, str, str, str, str, str]:
        return (
            self.match_date,
            self.match_time,
            self.home_team,
            self.away_team,
            self.market_type,
            self.selection_name,
        )


def build_comparison_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    try:
        if dataframe.empty:
            return build_missing_both_dataframe()

        sportsplus_entries: list[ComparableEntry] = []
        playtime_entries: list[ComparableEntry] = []
        market_not_comparable_rows: list[dict[str, str]] = []

        for row in dataframe.to_dict(orient="records"):
            source_site = clean_text(row.get("source_site", ""))
            raw_market_type = clean_text(row.get("market_type", ""))
            mapped_market_type = _map_market_type(source_site, raw_market_type)
            selection_name = clean_text(row.get("selection_name", ""))

            if mapped_market_type and selection_name:
                entry = ComparableEntry(
                    source_site=source_site,
                    scraped_at=clean_text(row.get("scraped_at", "")),
                    match_date=clean_text(row.get("match_date", "")),
                    match_time=clean_text(row.get("match_time", "")),
                    home_team=clean_text(row.get("home_team", "")),
                    away_team=clean_text(row.get("away_team", "")),
                    market_type=mapped_market_type,
                    selection_name=selection_name,
                    handicap_or_line=_normalize_line(clean_text(row.get("handicap_or_line", ""))),
                    odds=clean_text(row.get("odds", "")),
                )
                if source_site == "sportsplus":
                    sportsplus_entries.append(entry)
                elif source_site == "playtime":
                    playtime_entries.append(entry)
            elif source_site == "playtime" and raw_market_type:
                market_not_comparable_rows.append(
                    _build_non_matched_row(
                        scraped_at=clean_text(row.get("scraped_at", "")) or current_timestamp(),
                        match_date=clean_text(row.get("match_date", "")),
                        match_time=clean_text(row.get("match_time", "")),
                        home_team=clean_text(row.get("home_team", "")),
                        away_team=clean_text(row.get("away_team", "")),
                        market_type=raw_market_type,
                        selection_name=selection_name,
                        handicap_or_line=clean_text(row.get("handicap_or_line", "")),
                        sportsplus_odds="",
                        playtime_odds=clean_text(row.get("odds", "")),
                        comparison_status="market_not_comparable",
                    )
                )

        rows: list[dict[str, str]] = []
        sportsplus_by_key = _group_by_base_key(sportsplus_entries)
        playtime_by_key = _group_by_base_key(playtime_entries)
        all_base_keys = sorted(set(sportsplus_by_key) | set(playtime_by_key))

        for base_key in all_base_keys:
            sp_entries = sportsplus_by_key.get(base_key, [])
            pt_entries = playtime_by_key.get(base_key, [])
            sp_by_line = {entry.handicap_or_line: entry for entry in sp_entries}
            pt_by_line = {entry.handicap_or_line: entry for entry in pt_entries}

            shared_lines = sorted(set(sp_by_line) & set(pt_by_line))
            for line in shared_lines:
                rows.append(_build_matched_row(sp_by_line.pop(line), pt_by_line.pop(line)))

            remaining_sp = list(sp_by_line.values())
            remaining_pt = list(pt_by_line.values())

            if remaining_sp and remaining_pt:
                for sp_entry, pt_entry in zip_longest(
                    sorted(remaining_sp, key=lambda item: item.handicap_or_line),
                    sorted(remaining_pt, key=lambda item: item.handicap_or_line),
                ):
                    context_entry = sp_entry or pt_entry
                    assert context_entry is not None
                    rows.append(
                        _build_non_matched_row(
                            scraped_at=context_entry.scraped_at or current_timestamp(),
                            match_date=context_entry.match_date,
                            match_time=context_entry.match_time,
                            home_team=context_entry.home_team,
                            away_team=context_entry.away_team,
                            market_type=context_entry.market_type,
                            selection_name=context_entry.selection_name,
                            handicap_or_line=sp_entry.handicap_or_line if sp_entry else pt_entry.handicap_or_line,
                            sportsplus_odds=sp_entry.odds if sp_entry else "",
                            playtime_odds=pt_entry.odds if pt_entry else "",
                            comparison_status="line_mismatch",
                        )
                    )
                continue

            for sp_entry in remaining_sp:
                rows.append(
                    _build_non_matched_row(
                        scraped_at=sp_entry.scraped_at,
                        match_date=sp_entry.match_date,
                        match_time=sp_entry.match_time,
                        home_team=sp_entry.home_team,
                        away_team=sp_entry.away_team,
                        market_type=sp_entry.market_type,
                        selection_name=sp_entry.selection_name,
                        handicap_or_line=sp_entry.handicap_or_line,
                        sportsplus_odds=sp_entry.odds,
                        playtime_odds="",
                        comparison_status="missing_playtime",
                    )
                )

            for pt_entry in remaining_pt:
                rows.append(
                    _build_non_matched_row(
                        scraped_at=pt_entry.scraped_at,
                        match_date=pt_entry.match_date,
                        match_time=pt_entry.match_time,
                        home_team=pt_entry.home_team,
                        away_team=pt_entry.away_team,
                        market_type=pt_entry.market_type,
                        selection_name=pt_entry.selection_name,
                        handicap_or_line=pt_entry.handicap_or_line,
                        sportsplus_odds="",
                        playtime_odds=pt_entry.odds,
                        comparison_status="missing_sportsplus",
                    )
                )

        rows.extend(market_not_comparable_rows)

        if not rows:
            rows.append(_build_missing_both_row())

        return pd.DataFrame(rows, columns=COMPARISON_COLUMNS).fillna("")
    except Exception as error:  # noqa: BLE001
        raise ComparisonError("Failed to build comparison dataframe") from error


def build_missing_both_dataframe() -> pd.DataFrame:
    return pd.DataFrame([_build_missing_both_row()], columns=COMPARISON_COLUMNS).fillna("")


def _group_by_base_key(entries: list[ComparableEntry]) -> dict[tuple[str, str, str, str, str, str], list[ComparableEntry]]:
    grouped: dict[tuple[str, str, str, str, str, str], list[ComparableEntry]] = {}
    for entry in entries:
        grouped.setdefault(entry.base_key, []).append(entry)
    return grouped


def _map_market_type(source_site: str, market_type: str) -> str:
    if source_site == "sportsplus":
        return SPORTSPLUS_MARKET_MAP.get(market_type, "")
    if source_site == "playtime":
        return PLAYTIME_MARKET_MAP.get(market_type, "")
    return ""


def _normalize_line(value: str) -> str:
    return clean_text(value).replace(" ", "")


def _build_matched_row(sportsplus_entry: ComparableEntry, playtime_entry: ComparableEntry) -> dict[str, str]:
    return {
        "scraped_at": sportsplus_entry.scraped_at or playtime_entry.scraped_at or current_timestamp(),
        "match_date": sportsplus_entry.match_date or playtime_entry.match_date,
        "match_time": sportsplus_entry.match_time or playtime_entry.match_time,
        "home_team": sportsplus_entry.home_team or playtime_entry.home_team,
        "away_team": sportsplus_entry.away_team or playtime_entry.away_team,
        "market_type": sportsplus_entry.market_type,
        "selection_name": sportsplus_entry.selection_name,
        "handicap_or_line": sportsplus_entry.handicap_or_line or playtime_entry.handicap_or_line,
        "sportsplus_odds": sportsplus_entry.odds,
        "playtime_odds": playtime_entry.odds,
        "stake_amount": _format_decimal(STAKE_AMOUNT),
        "sportsplus_return": _compute_return(sportsplus_entry.odds),
        "playtime_return": _compute_return(playtime_entry.odds),
        "comparison_status": "matched",
    }


def _build_non_matched_row(
    *,
    scraped_at: str,
    match_date: str,
    match_time: str,
    home_team: str,
    away_team: str,
    market_type: str,
    selection_name: str,
    handicap_or_line: str,
    sportsplus_odds: str,
    playtime_odds: str,
    comparison_status: str,
) -> dict[str, str]:
    return {
        "scraped_at": scraped_at or current_timestamp(),
        "match_date": match_date,
        "match_time": match_time,
        "home_team": home_team,
        "away_team": away_team,
        "market_type": market_type,
        "selection_name": selection_name,
        "handicap_or_line": handicap_or_line,
        "sportsplus_odds": sportsplus_odds,
        "playtime_odds": playtime_odds,
        "stake_amount": "",
        "sportsplus_return": "",
        "playtime_return": "",
        "comparison_status": comparison_status,
    }


def _build_missing_both_row() -> dict[str, str]:
    return _build_non_matched_row(
        scraped_at=current_timestamp(),
        match_date="",
        match_time="",
        home_team="",
        away_team="",
        market_type="",
        selection_name="",
        handicap_or_line="",
        sportsplus_odds="",
        playtime_odds="",
        comparison_status="missing_both",
    )


def _compute_return(odds_value: str) -> str:
    try:
        odds = float(clean_text(odds_value))
    except ValueError:
        return ""
    return _format_decimal(odds * STAKE_AMOUNT)


def _format_decimal(value: float) -> str:
    return f"{value:.2f}"
