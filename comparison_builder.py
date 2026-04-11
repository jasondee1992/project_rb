from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from exceptions import ComparisonError
from utils import clean_text, current_timestamp


COMPARISON_COLUMNS = [
    "match",
    "market",
    "line",
    "best_pair",
    "sportsplus_odds",
    "playtime_odds",
    "arb_status",
    "possible_bet",
    "possible_return_if_side_1_wins",
    "possible_return_if_side_2_wins",
    "guaranteed_profit",
    "profit_percent",
    "remarks",
    "scraped_at",
]

DETAILED_COMPARISON_COLUMNS = [
    "match_key",
    "home_team",
    "away_team",
    "normalized_market",
    "selection_side_1",
    "selection_side_2",
    "line_value",
    "source_1",
    "source_2",
    "odds_1",
    "odds_2",
    "arb_sum",
    "arb_status",
    "total_stake",
    "recommended_stake_1",
    "recommended_stake_2",
    "payout_if_side_1_wins",
    "payout_if_side_2_wins",
    "profit_if_side_1_wins",
    "profit_if_side_2_wins",
    "guaranteed_profit",
    "guaranteed_profit_percent",
    "margin_label",
    "data_quality_label",
    "risk_notes",
    "scraped_at",
]

TOLERANCE = 1e-6
SUPPORTED_MARKETS = {
    "Handicap (Incl. Overtime)": "handicap_incl_ot",
    "Over / Under (Incl. Overtime)": "over_under_incl_ot",
    "Winner (Incl. Overtime)": "winner_incl_ot",
    "Game HDP": "handicap_incl_ot",
    "Game OU": "over_under_incl_ot",
    "Game ML": "winner_incl_ot",
}

MATCHED_ARB_STATUSES = {"positive_arb_candidate", "break_even_candidate", "no_arb"}
MISSING_STATUSES = {"missing_playtime", "missing_sportsplus", "missing_both"}
MISMATCH_STATUSES = {"line_mismatch", "market_not_comparable", "incomplete_pair"}
PLACEHOLDER_TEXT = "NBA not available as of this moment"


@dataclass(frozen=True)
class NormalizedEntry:
    source_label: str
    source_site: str
    scraped_at: str
    match_date: str
    match_time: str
    home_team: str
    away_team: str
    normalized_market: str
    selection_name: str
    selection_side: str
    side_token: str
    line_value: str
    signed_line: float | None
    odds_text: str
    odds_value: float | None

    @property
    def match_key(self) -> str:
        return " | ".join(
            value
            for value in (
                clean_text(self.match_date),
                clean_text(self.match_time),
                clean_text(self.home_team),
                clean_text(self.away_team),
            )
            if value
        )

    @property
    def group_key(self) -> tuple[str, str]:
        return self.match_key, self.normalized_market

    @property
    def unique_key(self) -> tuple[str, str, str, str, str, str]:
        return (
            self.source_site,
            self.match_key,
            self.normalized_market,
            self.selection_side,
            self.line_value,
            self.odds_text,
        )


def build_comparison_dataframe(
    dataframe: pd.DataFrame,
    total_stake: float = 100.0,
    min_guaranteed_profit: float = 20.0,
    min_guaranteed_profit_percent: float = 1.0,
    max_stake_per_side: float = 1500.0,
) -> pd.DataFrame:
    detailed_dataframe = _build_detailed_comparison_dataframe(
        dataframe=dataframe,
        total_stake=total_stake,
        min_guaranteed_profit=min_guaranteed_profit,
        min_guaranteed_profit_percent=min_guaranteed_profit_percent,
        max_stake_per_side=max_stake_per_side,
    )
    return _build_simple_comparison_dataframe(detailed_dataframe)


def _build_detailed_comparison_dataframe(
    *,
    dataframe: pd.DataFrame,
    total_stake: float,
    min_guaranteed_profit: float,
    min_guaranteed_profit_percent: float,
    max_stake_per_side: float,
) -> pd.DataFrame:
    try:
        if dataframe.empty:
            return _build_detailed_missing_both_dataframe(total_stake=total_stake)

        valid_entries: list[NormalizedEntry] = []
        rows: list[dict[str, str]] = []
        placeholder_sources = _detect_placeholder_sources(dataframe)

        for raw_row in dataframe.to_dict(orient="records"):
            normalization = _normalize_row(raw_row)
            entry = normalization.get("entry")
            if entry is not None:
                valid_entries.append(entry)
                continue

            status = clean_text(normalization.get("arb_status", ""))
            if status:
                rows.append(
                    _build_status_row(
                        match_key=clean_text(normalization.get("match_key", "")),
                        home_team=clean_text(normalization.get("home_team", "")),
                        away_team=clean_text(normalization.get("away_team", "")),
                        normalized_market=clean_text(normalization.get("normalized_market", "")),
                        selection_side_1=clean_text(normalization.get("selection_side_1", "")),
                        selection_side_2=clean_text(normalization.get("selection_side_2", "")),
                        line_value=clean_text(normalization.get("line_value", "")),
                        source_1=clean_text(normalization.get("source_1", "")),
                        source_2=clean_text(normalization.get("source_2", "")),
                        odds_1=clean_text(normalization.get("odds_1", "")),
                        odds_2=clean_text(normalization.get("odds_2", "")),
                        arb_status=status,
                        risk_notes=_build_status_risk_notes(status, placeholder_other_source=False),
                        scraped_at=clean_text(normalization.get("scraped_at", "")) or current_timestamp(),
                        total_stake=total_stake,
                        data_quality_label=_data_quality_label_for_status(status, placeholder_other_source=False),
                    )
                )

        grouped: dict[tuple[str, str], dict[str, list[NormalizedEntry]]] = {}
        for entry in valid_entries:
            source_bucket = grouped.setdefault(entry.group_key, {"sportsplus": [], "playtime": []})
            source_bucket.setdefault(entry.source_site, []).append(entry)

        for group_key in sorted(grouped):
            source_bucket = grouped[group_key]
            sportsplus_entries = source_bucket.get("sportsplus", [])
            playtime_entries = source_bucket.get("playtime", [])
            matched_playtime_keys: set[tuple[str, str, str, str, str, str]] = set()

            for sportsplus_entry in sorted(sportsplus_entries, key=_entry_sort_key):
                opposite_candidates = [
                    candidate
                    for candidate in playtime_entries
                    if candidate.unique_key not in matched_playtime_keys and _is_strict_opposite_pair(sportsplus_entry, candidate)
                ]
                if opposite_candidates:
                    playtime_entry = max(
                        opposite_candidates,
                        key=lambda item: item.odds_value if item.odds_value is not None else -1.0,
                    )
                    matched_playtime_keys.add(playtime_entry.unique_key)
                    rows.append(
                        _build_matched_arb_row(
                            sportsplus_entry,
                            playtime_entry,
                            total_stake=total_stake,
                            min_guaranteed_profit=min_guaranteed_profit,
                            min_guaranteed_profit_percent=min_guaranteed_profit_percent,
                            max_stake_per_side=max_stake_per_side,
                        )
                    )
                    continue

                rows.append(
                    _build_unmatched_row(
                        entry=sportsplus_entry,
                        other_source_entries=playtime_entries,
                        missing_status="missing_playtime",
                        other_source_label="PLAYTIME",
                        total_stake=total_stake,
                        placeholder_other_source="playtime" in placeholder_sources,
                    )
                )

            for playtime_entry in sorted(playtime_entries, key=_entry_sort_key):
                if playtime_entry.unique_key in matched_playtime_keys:
                    continue
                rows.append(
                    _build_unmatched_row(
                        entry=playtime_entry,
                        other_source_entries=sportsplus_entries,
                        missing_status="missing_sportsplus",
                        other_source_label="SPORTS PLUS",
                        total_stake=total_stake,
                        placeholder_other_source="sportsplus" in placeholder_sources,
                    )
                )

        if not rows:
            rows.append(_build_missing_both_row(total_stake=total_stake))

        return pd.DataFrame(rows, columns=DETAILED_COMPARISON_COLUMNS).fillna("")
    except Exception as error:  # noqa: BLE001
        raise ComparisonError("Failed to build arbitrage comparison dataframe") from error


def build_missing_both_dataframe(total_stake: float = 100.0) -> pd.DataFrame:
    return _build_simple_comparison_dataframe(_build_detailed_missing_both_dataframe(total_stake=total_stake))


def _build_detailed_missing_both_dataframe(total_stake: float) -> pd.DataFrame:
    return pd.DataFrame([_build_missing_both_row(total_stake=total_stake)], columns=DETAILED_COMPARISON_COLUMNS).fillna("")


def summarize_comparison_dataframe(dataframe: pd.DataFrame) -> dict[str, int]:
    if dataframe.empty or "arb_status" not in dataframe.columns:
        return {
            "comparable_pairs": 0,
            "positive_arb_candidates": 0,
            "break_even_candidates": 0,
            "no_arb_rows": 0,
            "mismatch_missing_rows": 0,
        }

    statuses = dataframe["arb_status"].astype(str)
    return {
        "comparable_pairs": int(statuses.isin(MATCHED_ARB_STATUSES).sum()),
        "positive_arb_candidates": int((statuses == "positive_arb_candidate").sum()),
        "break_even_candidates": int((statuses == "break_even_candidate").sum()),
        "no_arb_rows": int((statuses == "no_arb").sum()),
        "mismatch_missing_rows": int(statuses.isin(MISMATCH_STATUSES | MISSING_STATUSES).sum()),
    }


def _build_simple_comparison_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return pd.DataFrame(columns=COMPARISON_COLUMNS).fillna("")

    simple_rows = [_simplify_comparison_row(row) for row in dataframe.to_dict(orient="records")]
    simple_dataframe = pd.DataFrame(simple_rows, columns=COMPARISON_COLUMNS).fillna("")
    simple_dataframe["__sort_status"] = simple_dataframe["arb_status"].map(_status_sort_priority).fillna(99)
    simple_dataframe["__sort_profit"] = pd.to_numeric(simple_dataframe["guaranteed_profit"], errors="coerce").fillna(-10_000.0)
    simple_dataframe["__sort_match"] = simple_dataframe["match"].astype(str)
    simple_dataframe = simple_dataframe.sort_values(
        by=["__sort_status", "__sort_profit", "__sort_match", "market", "line"],
        ascending=[True, False, True, True, True],
    ).drop(columns=["__sort_status", "__sort_profit", "__sort_match"])
    return simple_dataframe.reset_index(drop=True).reindex(columns=COMPARISON_COLUMNS).fillna("")


def _simplify_comparison_row(row: dict[str, Any]) -> dict[str, str]:
    source_1 = clean_text(row.get("source_1", ""))
    source_2 = clean_text(row.get("source_2", ""))
    odds_1 = clean_text(row.get("odds_1", ""))
    odds_2 = clean_text(row.get("odds_2", ""))
    sportsplus_odds = odds_1 if source_1 == "SPORTS PLUS" else odds_2 if source_2 == "SPORTS PLUS" else ""
    playtime_odds = odds_1 if source_1 == "PLAYTIME" else odds_2 if source_2 == "PLAYTIME" else ""
    arb_status = clean_text(row.get("arb_status", ""))
    has_valid_pair = arb_status in MATCHED_ARB_STATUSES

    return {
        "match": _simple_match_label(clean_text(row.get("home_team", "")), clean_text(row.get("away_team", ""))),
        "market": _simple_market_label(clean_text(row.get("normalized_market", ""))),
        "line": _simple_line_value(clean_text(row.get("normalized_market", "")), clean_text(row.get("line_value", ""))),
        "best_pair": _simple_best_pair(
            clean_text(row.get("selection_side_1", "")),
            clean_text(row.get("selection_side_2", "")),
        ),
        "sportsplus_odds": sportsplus_odds,
        "playtime_odds": playtime_odds,
        "arb_status": arb_status,
        "possible_bet": "100",
        "possible_return_if_side_1_wins": clean_text(row.get("payout_if_side_1_wins", "")) if has_valid_pair else "",
        "possible_return_if_side_2_wins": clean_text(row.get("payout_if_side_2_wins", "")) if has_valid_pair else "",
        "guaranteed_profit": clean_text(row.get("guaranteed_profit", "")) if has_valid_pair else "",
        "profit_percent": clean_text(row.get("guaranteed_profit_percent", "")) if has_valid_pair else "",
        "remarks": _simple_remarks(arb_status),
        "scraped_at": clean_text(row.get("scraped_at", "")),
    }


def _status_sort_priority(status: str) -> int:
    priorities = {
        "positive_arb_candidate": 0,
        "break_even_candidate": 1,
        "no_arb": 2,
        "line_mismatch": 3,
        "incomplete_pair": 4,
        "market_not_comparable": 5,
        "missing_playtime": 6,
        "missing_sportsplus": 7,
        "missing_both": 8,
    }
    return priorities.get(clean_text(status), 99)


def _simple_match_label(home_team: str, away_team: str) -> str:
    if home_team and away_team:
        return f"{home_team} vs {away_team}"
    return home_team or away_team


def _simple_market_label(normalized_market: str) -> str:
    return {
        "winner_incl_ot": "Winner",
        "handicap_incl_ot": "Handicap",
        "over_under_incl_ot": "Over/Under",
    }.get(normalized_market, clean_text(normalized_market))


def _simple_line_value(normalized_market: str, line_value: str) -> str:
    return "" if normalized_market == "winner_incl_ot" else line_value


def _simple_best_pair(selection_side_1: str, selection_side_2: str) -> str:
    if selection_side_1 and selection_side_2:
        return f"{selection_side_1} / {selection_side_2}"
    return selection_side_1 or selection_side_2


def _simple_remarks(arb_status: str) -> str:
    return {
        "positive_arb_candidate": "candidate only; verify manually",
        "break_even_candidate": "candidate only; verify manually",
        "no_arb": "no arb; verify manually",
        "line_mismatch": "line mismatch",
        "missing_playtime": "playtime missing",
        "missing_sportsplus": "sportsplus missing",
        "missing_both": "both sources missing",
        "incomplete_pair": "incomplete pair",
        "market_not_comparable": "not comparable",
    }.get(arb_status, "verify manually")


def _detect_placeholder_sources(dataframe: pd.DataFrame) -> set[str]:
    if dataframe.empty:
        return set()

    source_sites = dataframe.get("source_site", pd.Series(dtype=str)).astype(str)
    raw_texts = dataframe.get("raw_text", pd.Series(dtype=str)).astype(str)
    placeholder_mask = raw_texts.str.contains(PLACEHOLDER_TEXT, na=False)
    return {clean_text(source_site).lower() for source_site in source_sites[placeholder_mask] if clean_text(source_site)}


def _normalize_row(raw_row: dict[str, Any]) -> dict[str, Any]:
    source_label = clean_text(raw_row.get("source_label", ""))
    source_site = clean_text(raw_row.get("source_site", ""))
    match_date = clean_text(raw_row.get("match_date", ""))
    match_time = clean_text(raw_row.get("match_time", ""))
    home_team = clean_text(raw_row.get("home_team", ""))
    away_team = clean_text(raw_row.get("away_team", ""))
    market_type = clean_text(raw_row.get("market_type", ""))
    selection_name = clean_text(raw_row.get("selection_name", ""))
    line_raw = clean_text(raw_row.get("handicap_or_line", ""))
    odds_text = clean_text(raw_row.get("odds", ""))
    scraped_at = clean_text(raw_row.get("scraped_at", "")) or current_timestamp()
    match_key = _build_match_key(match_date, match_time, home_team, away_team)
    normalized_market = SUPPORTED_MARKETS.get(market_type, "")

    if not market_type:
        return {}

    if not normalized_market:
        return {
            "arb_status": "market_not_comparable",
            "match_key": match_key,
            "home_team": home_team,
            "away_team": away_team,
            "normalized_market": "",
            "selection_side_1": selection_name,
            "selection_side_2": "",
            "line_value": line_raw,
            "source_1": source_label,
            "source_2": "",
            "odds_1": odds_text,
            "odds_2": "",
            "risk_notes": "not a true opposite market",
            "scraped_at": scraped_at,
        }

    odds_value = _parse_odds_value(odds_text)
    if odds_value is None:
        return {
            "arb_status": "incomplete_pair",
            "match_key": match_key,
            "home_team": home_team,
            "away_team": away_team,
            "normalized_market": normalized_market,
            "selection_side_1": selection_name,
            "selection_side_2": "",
            "line_value": line_raw,
            "source_1": source_label,
            "source_2": "",
            "odds_1": odds_text,
            "odds_2": "",
            "risk_notes": "incomplete pair",
            "scraped_at": scraped_at,
        }

    if normalized_market == "winner_incl_ot":
        side_token = _team_side_token(selection_name, home_team, away_team)
        if not side_token:
            return {
                "arb_status": "market_not_comparable",
                "match_key": match_key,
                "home_team": home_team,
                "away_team": away_team,
                "normalized_market": normalized_market,
                "selection_side_1": selection_name,
                "selection_side_2": "",
                "line_value": "",
                "source_1": source_label,
                "source_2": "",
                "odds_1": odds_text,
                "odds_2": "",
                "risk_notes": "not a true opposite market",
                "scraped_at": scraped_at,
            }
        return {
            "entry": NormalizedEntry(
                source_label=source_label,
                source_site=source_site,
                scraped_at=scraped_at,
                match_date=match_date,
                match_time=match_time,
                home_team=home_team,
                away_team=away_team,
                normalized_market=normalized_market,
                selection_name=selection_name,
                selection_side=selection_name,
                side_token=side_token,
                line_value="",
                signed_line=None,
                odds_text=odds_text,
                odds_value=odds_value,
            )
        }

    if normalized_market == "over_under_incl_ot":
        selection_token = selection_name.strip().lower()
        if selection_token not in {"over", "under"}:
            return {
                "arb_status": "market_not_comparable",
                "match_key": match_key,
                "home_team": home_team,
                "away_team": away_team,
                "normalized_market": normalized_market,
                "selection_side_1": selection_name,
                "selection_side_2": "",
                "line_value": line_raw,
                "source_1": source_label,
                "source_2": "",
                "odds_1": odds_text,
                "odds_2": "",
                "risk_notes": "not a true opposite market",
                "scraped_at": scraped_at,
            }
        line_value = _normalize_numeric_string(line_raw)
        if not line_value:
            return {
                "arb_status": "incomplete_pair",
                "match_key": match_key,
                "home_team": home_team,
                "away_team": away_team,
                "normalized_market": normalized_market,
                "selection_side_1": selection_name,
                "selection_side_2": "",
                "line_value": line_raw,
                "source_1": source_label,
                "source_2": "",
                "odds_1": odds_text,
                "odds_2": "",
                "risk_notes": "incomplete pair",
                "scraped_at": scraped_at,
            }
        return {
            "entry": NormalizedEntry(
                source_label=source_label,
                source_site=source_site,
                scraped_at=scraped_at,
                match_date=match_date,
                match_time=match_time,
                home_team=home_team,
                away_team=away_team,
                normalized_market=normalized_market,
                selection_name=selection_name,
                selection_side=selection_name.title(),
                side_token=selection_token,
                line_value=line_value,
                signed_line=None,
                odds_text=odds_text,
                odds_value=odds_value,
            )
        }

    if normalized_market == "handicap_incl_ot":
        side_token = _team_side_token(selection_name, home_team, away_team)
        signed_line = _parse_signed_line(line_raw)
        if not side_token or signed_line is None:
            return {
                "arb_status": "incomplete_pair",
                "match_key": match_key,
                "home_team": home_team,
                "away_team": away_team,
                "normalized_market": normalized_market,
                "selection_side_1": _build_handicap_display(selection_name, line_raw),
                "selection_side_2": "",
                "line_value": _normalize_numeric_string(line_raw),
                "source_1": source_label,
                "source_2": "",
                "odds_1": odds_text,
                "odds_2": "",
                "risk_notes": "incomplete pair",
                "scraped_at": scraped_at,
            }
        return {
            "entry": NormalizedEntry(
                source_label=source_label,
                source_site=source_site,
                scraped_at=scraped_at,
                match_date=match_date,
                match_time=match_time,
                home_team=home_team,
                away_team=away_team,
                normalized_market=normalized_market,
                selection_name=selection_name,
                selection_side=_build_handicap_display(selection_name, _format_signed_line(signed_line)),
                side_token=side_token,
                line_value=_format_line_value(abs(signed_line)),
                signed_line=signed_line,
                odds_text=odds_text,
                odds_value=odds_value,
            )
        }

    return {}


def _build_match_key(match_date: str, match_time: str, home_team: str, away_team: str) -> str:
    return " | ".join(value for value in (match_date, match_time, home_team, away_team) if value)


def _entry_sort_key(entry: NormalizedEntry) -> tuple[str, str, str, str]:
    signed_line = _format_signed_line(entry.signed_line) if entry.signed_line is not None else ""
    return entry.normalized_market, entry.line_value, entry.selection_side, signed_line


def _team_side_token(selection_name: str, home_team: str, away_team: str) -> str:
    selection_lower = selection_name.lower()
    if selection_lower == home_team.lower():
        return "home"
    if selection_lower == away_team.lower():
        return "away"
    return ""


def _parse_odds_value(odds_text: str) -> float | None:
    try:
        odds_value = float(clean_text(odds_text))
    except (TypeError, ValueError):
        return None
    return odds_value if odds_value > 1 else None


def _parse_signed_line(line_raw: str) -> float | None:
    normalized = clean_text(line_raw).replace(" ", "")
    if not normalized:
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def _normalize_numeric_string(raw_value: str) -> str:
    signed_line = _parse_signed_line(raw_value)
    if signed_line is None:
        return ""
    return _format_line_value(signed_line)


def _format_line_value(value: float) -> str:
    integer_value = int(value)
    if abs(value - integer_value) < TOLERANCE:
        return str(integer_value)
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _format_signed_line(value: float | None) -> str:
    if value is None:
        return ""
    line_value = _format_line_value(abs(value))
    sign = "+" if value >= 0 else "-"
    return f"{sign}{line_value}"


def _build_handicap_display(selection_name: str, line_text: str) -> str:
    cleaned_line = clean_text(line_text).replace(" ", "")
    return clean_text(f"{selection_name} {cleaned_line}")


def _is_strict_opposite_pair(entry_one: NormalizedEntry, entry_two: NormalizedEntry) -> bool:
    if entry_one.match_key != entry_two.match_key:
        return False
    if entry_one.normalized_market != entry_two.normalized_market:
        return False
    if entry_one.source_site == entry_two.source_site:
        return False
    if entry_one.odds_value is None or entry_two.odds_value is None:
        return False

    if entry_one.normalized_market == "winner_incl_ot":
        return {entry_one.side_token, entry_two.side_token} == {"home", "away"}

    if entry_one.normalized_market == "over_under_incl_ot":
        return (
            entry_one.line_value
            and entry_one.line_value == entry_two.line_value
            and {entry_one.side_token, entry_two.side_token} == {"over", "under"}
        )

    if entry_one.normalized_market == "handicap_incl_ot":
        if entry_one.signed_line is None or entry_two.signed_line is None:
            return False
        return (
            entry_one.line_value
            and entry_one.line_value == entry_two.line_value
            and {entry_one.side_token, entry_two.side_token} == {"home", "away"}
            and abs(entry_one.signed_line + entry_two.signed_line) < TOLERANCE
        )

    return False


def _build_matched_arb_row(
    entry_one: NormalizedEntry,
    entry_two: NormalizedEntry,
    *,
    total_stake: float,
    min_guaranteed_profit: float,
    min_guaranteed_profit_percent: float,
    max_stake_per_side: float,
) -> dict[str, str]:
    assert entry_one.odds_value is not None
    assert entry_two.odds_value is not None

    arb_sum = (1 / entry_one.odds_value) + (1 / entry_two.odds_value)
    stake_one = total_stake * ((1 / entry_one.odds_value) / arb_sum)
    stake_two = total_stake * ((1 / entry_two.odds_value) / arb_sum)
    payout_one = stake_one * entry_one.odds_value
    payout_two = stake_two * entry_two.odds_value
    profit_one = payout_one - total_stake
    profit_two = payout_two - total_stake
    guaranteed_profit = min(profit_one, profit_two)
    guaranteed_profit_percent = (guaranteed_profit / total_stake) * 100 if total_stake else 0.0

    if abs(arb_sum - 1) <= TOLERANCE:
        arb_status = "break_even_candidate"
    elif arb_sum < 1:
        arb_status = "positive_arb_candidate"
    else:
        arb_status = "no_arb"

    exceeds_max_stake = (
        recommended_stake_exceeds_limit(stake_one, max_stake_per_side)
        or recommended_stake_exceeds_limit(stake_two, max_stake_per_side)
    )
    below_profit_threshold = guaranteed_profit + TOLERANCE < min_guaranteed_profit
    below_profit_percent_threshold = guaranteed_profit_percent + TOLERANCE < min_guaranteed_profit_percent
    needs_manual_check = arb_status in {"positive_arb_candidate", "break_even_candidate"} and (
        exceeds_max_stake or below_profit_threshold or below_profit_percent_threshold
    )
    margin_label = _margin_label(guaranteed_profit_percent)
    data_quality_label = "NEEDS_MANUAL_CHECK" if needs_manual_check else "CLEAN_MATCH"
    risk_notes = _build_matched_risk_notes(
        arb_status=arb_status,
        exceeds_max_stake=exceeds_max_stake,
        below_profit_threshold=below_profit_threshold,
        below_profit_percent_threshold=below_profit_percent_threshold,
    )

    return {
        "match_key": entry_one.match_key or entry_two.match_key,
        "home_team": entry_one.home_team or entry_two.home_team,
        "away_team": entry_one.away_team or entry_two.away_team,
        "normalized_market": entry_one.normalized_market,
        "selection_side_1": entry_one.selection_side,
        "selection_side_2": entry_two.selection_side,
        "line_value": entry_one.line_value or entry_two.line_value,
        "source_1": entry_one.source_label,
        "source_2": entry_two.source_label,
        "odds_1": entry_one.odds_text,
        "odds_2": entry_two.odds_text,
        "arb_sum": _format_metric(arb_sum),
        "arb_status": arb_status,
        "total_stake": _format_money(total_stake),
        "recommended_stake_1": _format_money(stake_one),
        "recommended_stake_2": _format_money(stake_two),
        "payout_if_side_1_wins": _format_money(payout_one),
        "payout_if_side_2_wins": _format_money(payout_two),
        "profit_if_side_1_wins": _format_money(profit_one),
        "profit_if_side_2_wins": _format_money(profit_two),
        "guaranteed_profit": _format_money(guaranteed_profit),
        "guaranteed_profit_percent": _format_percent(guaranteed_profit_percent),
        "margin_label": margin_label,
        "data_quality_label": data_quality_label,
        "risk_notes": risk_notes,
        "scraped_at": current_timestamp(),
    }


def _build_unmatched_row(
    *,
    entry: NormalizedEntry,
    other_source_entries: list[NormalizedEntry],
    missing_status: str,
    other_source_label: str,
    total_stake: float,
    placeholder_other_source: bool,
) -> dict[str, str]:
    if not other_source_entries:
        return _build_status_row(
            match_key=entry.match_key,
            home_team=entry.home_team,
            away_team=entry.away_team,
            normalized_market=entry.normalized_market,
            selection_side_1=entry.selection_side if missing_status == "missing_playtime" else "",
            selection_side_2="" if missing_status == "missing_playtime" else entry.selection_side,
            line_value=entry.line_value,
            source_1=entry.source_label if missing_status == "missing_playtime" else "",
            source_2="" if missing_status == "missing_playtime" else entry.source_label,
            odds_1=entry.odds_text if missing_status == "missing_playtime" else "",
            odds_2="" if missing_status == "missing_playtime" else entry.odds_text,
            arb_status=missing_status,
            risk_notes=_build_status_risk_notes(missing_status, placeholder_other_source=placeholder_other_source),
            scraped_at=entry.scraped_at,
            total_stake=total_stake,
            data_quality_label="PLACEHOLDER_SOURCE" if placeholder_other_source else "MISSING_ONE_SIDE",
            fallback_selection_side=_expected_opposite_display(entry),
            fallback_source_label=other_source_label,
        )

    opposite_side_entries = [candidate for candidate in other_source_entries if _is_opposite_side(entry, candidate)]
    if _has_line_mismatch(entry, opposite_side_entries):
        opposite_display = opposite_side_entries[0].selection_side if opposite_side_entries else _expected_opposite_display(entry)
        mismatch_line_value = _mismatch_line_value(entry.line_value, opposite_side_entries[0].line_value if opposite_side_entries else "")
        return _build_status_row(
            match_key=entry.match_key,
            home_team=entry.home_team,
            away_team=entry.away_team,
            normalized_market=entry.normalized_market,
            selection_side_1=entry.selection_side if missing_status == "missing_playtime" else opposite_display,
            selection_side_2=opposite_display if missing_status == "missing_playtime" else entry.selection_side,
            line_value=mismatch_line_value,
            source_1=entry.source_label if missing_status == "missing_playtime" else opposite_side_entries[0].source_label,
            source_2=opposite_side_entries[0].source_label if missing_status == "missing_playtime" else entry.source_label,
            odds_1=entry.odds_text if missing_status == "missing_playtime" else opposite_side_entries[0].odds_text,
            odds_2=opposite_side_entries[0].odds_text if missing_status == "missing_playtime" else entry.odds_text,
            arb_status="line_mismatch",
            risk_notes=_build_status_risk_notes("line_mismatch", placeholder_other_source=placeholder_other_source),
            scraped_at=entry.scraped_at,
            total_stake=total_stake,
            data_quality_label="LINE_MISMATCH",
        )

    if opposite_side_entries:
        opposite_display = opposite_side_entries[0].selection_side
        return _build_status_row(
            match_key=entry.match_key,
            home_team=entry.home_team,
            away_team=entry.away_team,
            normalized_market=entry.normalized_market,
            selection_side_1=entry.selection_side if missing_status == "missing_playtime" else opposite_display,
            selection_side_2=opposite_display if missing_status == "missing_playtime" else entry.selection_side,
            line_value=entry.line_value,
            source_1=entry.source_label if missing_status == "missing_playtime" else opposite_side_entries[0].source_label,
            source_2=opposite_side_entries[0].source_label if missing_status == "missing_playtime" else entry.source_label,
            odds_1=entry.odds_text if missing_status == "missing_playtime" else opposite_side_entries[0].odds_text,
            odds_2=opposite_side_entries[0].odds_text if missing_status == "missing_playtime" else entry.odds_text,
            arb_status="incomplete_pair",
            risk_notes=_build_status_risk_notes("incomplete_pair", placeholder_other_source=placeholder_other_source),
            scraped_at=entry.scraped_at,
            total_stake=total_stake,
            data_quality_label="INCOMPLETE_MARKET",
        )

    return _build_status_row(
        match_key=entry.match_key,
        home_team=entry.home_team,
        away_team=entry.away_team,
        normalized_market=entry.normalized_market,
        selection_side_1=entry.selection_side if missing_status == "missing_playtime" else "",
        selection_side_2="" if missing_status == "missing_playtime" else entry.selection_side,
        line_value=entry.line_value,
        source_1=entry.source_label if missing_status == "missing_playtime" else "",
        source_2="" if missing_status == "missing_playtime" else entry.source_label,
        odds_1=entry.odds_text if missing_status == "missing_playtime" else "",
        odds_2="" if missing_status == "missing_playtime" else entry.odds_text,
        arb_status="incomplete_pair",
        risk_notes=_build_status_risk_notes("incomplete_pair", placeholder_other_source=placeholder_other_source),
        scraped_at=entry.scraped_at,
        total_stake=total_stake,
        data_quality_label="PLACEHOLDER_SOURCE" if placeholder_other_source else "INCOMPLETE_MARKET",
        fallback_selection_side=_expected_opposite_display(entry),
        fallback_source_label=other_source_label,
    )


def _is_opposite_side(entry: NormalizedEntry, candidate: NormalizedEntry) -> bool:
    if entry.normalized_market != candidate.normalized_market:
        return False
    if entry.match_key != candidate.match_key:
        return False

    if entry.normalized_market == "winner_incl_ot":
        return {entry.side_token, candidate.side_token} == {"home", "away"}
    if entry.normalized_market == "over_under_incl_ot":
        return {entry.side_token, candidate.side_token} == {"over", "under"}
    if entry.normalized_market == "handicap_incl_ot":
        return {entry.side_token, candidate.side_token} == {"home", "away"}
    return False


def _has_line_mismatch(entry: NormalizedEntry, opposite_side_entries: list[NormalizedEntry]) -> bool:
    if not opposite_side_entries:
        return False
    if entry.normalized_market == "winner_incl_ot":
        return False
    if entry.normalized_market == "over_under_incl_ot":
        return not any(candidate.line_value == entry.line_value for candidate in opposite_side_entries)
    if entry.normalized_market == "handicap_incl_ot":
        if entry.signed_line is None:
            return True
        return not any(
            candidate.signed_line is not None and abs(entry.signed_line + candidate.signed_line) < TOLERANCE
            for candidate in opposite_side_entries
        )
    return False


def _mismatch_line_value(first_line: str, second_line: str) -> str:
    if first_line and second_line and first_line != second_line:
        return f"{first_line} vs {second_line}"
    return first_line or second_line


def _expected_opposite_display(entry: NormalizedEntry) -> str:
    if entry.normalized_market == "winner_incl_ot":
        return entry.away_team if entry.side_token == "home" else entry.home_team
    if entry.normalized_market == "over_under_incl_ot":
        return "Under" if entry.side_token == "over" else "Over"
    if entry.normalized_market == "handicap_incl_ot" and entry.signed_line is not None:
        opposite_team = entry.away_team if entry.side_token == "home" else entry.home_team
        return _build_handicap_display(opposite_team, _format_signed_line(-entry.signed_line))
    return ""


def _build_status_row(
    *,
    match_key: str,
    home_team: str,
    away_team: str,
    normalized_market: str,
    selection_side_1: str,
    selection_side_2: str,
    line_value: str,
    source_1: str,
    source_2: str,
    odds_1: str,
    odds_2: str,
    arb_status: str,
    risk_notes: str,
    scraped_at: str,
    total_stake: float,
    data_quality_label: str,
    fallback_selection_side: str = "",
    fallback_source_label: str = "",
) -> dict[str, str]:
    if not selection_side_2 and fallback_selection_side:
        selection_side_2 = fallback_selection_side
    if not source_2 and fallback_source_label:
        source_2 = fallback_source_label

    return {
        "match_key": match_key,
        "home_team": home_team,
        "away_team": away_team,
        "normalized_market": normalized_market,
        "selection_side_1": selection_side_1,
        "selection_side_2": selection_side_2,
        "line_value": line_value,
        "source_1": source_1,
        "source_2": source_2,
        "odds_1": odds_1,
        "odds_2": odds_2,
        "arb_sum": "",
        "arb_status": arb_status,
        "total_stake": _format_money(total_stake),
        "recommended_stake_1": "",
        "recommended_stake_2": "",
        "payout_if_side_1_wins": "",
        "payout_if_side_2_wins": "",
        "profit_if_side_1_wins": "",
        "profit_if_side_2_wins": "",
        "guaranteed_profit": "",
        "guaranteed_profit_percent": "",
        "margin_label": "NONE",
        "data_quality_label": data_quality_label,
        "risk_notes": risk_notes,
        "scraped_at": scraped_at or current_timestamp(),
    }


def _build_missing_both_row(total_stake: float) -> dict[str, str]:
    return _build_status_row(
        match_key="",
        home_team="",
        away_team="",
        normalized_market="",
        selection_side_1="",
        selection_side_2="",
        line_value="",
        source_1="SPORTS PLUS",
        source_2="PLAYTIME",
        odds_1="",
        odds_2="",
        arb_status="missing_both",
        risk_notes=_build_status_risk_notes("missing_both", placeholder_other_source=False),
        scraped_at=current_timestamp(),
        total_stake=total_stake,
        data_quality_label="INCOMPLETE_MARKET",
    )


def _margin_label(guaranteed_profit_percent: float) -> str:
    if guaranteed_profit_percent >= 2:
        return "HIGH_MARGIN"
    if guaranteed_profit_percent >= 1:
        return "MEDIUM_MARGIN"
    if guaranteed_profit_percent > 0:
        return "LOW_MARGIN"
    return "NONE"


def _data_quality_label_for_status(arb_status: str, *, placeholder_other_source: bool) -> str:
    if arb_status == "line_mismatch":
        return "LINE_MISMATCH"
    if arb_status in MISSING_STATUSES:
        return "PLACEHOLDER_SOURCE" if placeholder_other_source else "MISSING_ONE_SIDE"
    if arb_status in {"market_not_comparable", "incomplete_pair", "missing_both"}:
        return "INCOMPLETE_MARKET"
    return "NEEDS_MANUAL_CHECK"


def _build_status_risk_notes(arb_status: str, *, placeholder_other_source: bool) -> str:
    notes: list[str]
    if arb_status == "line_mismatch":
        notes = ["line mismatch across sources"]
    elif arb_status in {"missing_playtime", "missing_sportsplus"}:
        notes = ["other source currently returned placeholder data"] if placeholder_other_source else ["one side missing from other source"]
    elif arb_status == "missing_both":
        notes = ["no comparable source data in current cycle"]
    elif arb_status == "market_not_comparable":
        notes = ["market not comparable across sources"]
    else:
        notes = ["incomplete market data"]

    notes.extend(
        [
            "informational dashboard only",
            "verify manually before any action",
        ]
    )
    return "; ".join(notes)


def _build_matched_risk_notes(
    *,
    arb_status: str,
    exceeds_max_stake: bool,
    below_profit_threshold: bool,
    below_profit_percent_threshold: bool,
) -> str:
    if arb_status in {"positive_arb_candidate", "break_even_candidate"}:
        notes = [
            "candidate only",
            "verify manually before any action",
            "odds may move",
            "market may suspend",
            "max stake limits may differ",
            "rule differences may apply",
        ]
    else:
        notes = [
            "no arbitrage based on current odds",
            "informational dashboard only",
            "verify manually before any action",
            "odds may move",
        ]

    if arb_status in {"positive_arb_candidate", "break_even_candidate"}:
        if exceeds_max_stake:
            notes.append("stake split exceeds configured max per side")
        if below_profit_threshold:
            notes.append("below configured guaranteed profit threshold")
        if below_profit_percent_threshold:
            notes.append("below configured guaranteed profit percent threshold")
    return "; ".join(notes)


def recommended_stake_exceeds_limit(recommended_stake: float, max_stake_per_side: float) -> bool:
    return recommended_stake - max_stake_per_side > TOLERANCE


def _format_money(value: float) -> str:
    return f"{value:.2f}"


def _format_percent(value: float) -> str:
    return f"{value:.2f}"


def _format_metric(value: float) -> str:
    return f"{value:.6f}"
