from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


DATAFRAME_COLUMNS = [
    "scraped_at",
    "source_url",
    "category_name",
    "league_name",
    "sport_name",
    "match_date",
    "match_time",
    "home_team",
    "away_team",
    "match_status",
    "market_type",
    "selection_name",
    "odds",
    "handicap_or_line",
    "raw_text",
]

DEDUP_KEY_COLUMNS = [
    "match_date",
    "match_time",
    "home_team",
    "away_team",
    "market_type",
    "selection_name",
]


@dataclass(frozen=True)
class Settings:
    target_url: str
    headless: bool
    google_sheet_name: str
    google_worksheet_name: str
    google_creds_json: Path
    scroll_pause_ms: int
    max_scrolls: int
    output_dir: Path
    csv_output_path: Path
    debug_html_path: Path
    debug_screenshot_path: Path
    env_file_used: str | None


def load_settings() -> Settings:
    env_file_used = _load_first_env_file()
    output_dir = Path("output")

    return Settings(
        target_url=_get_required_env("TARGET_URL"),
        headless=_parse_bool(os.getenv("HEADLESS", "true")),
        google_sheet_name=_get_required_env("GOOGLE_SHEET_NAME"),
        google_worksheet_name=_get_required_env("GOOGLE_WORKSHEET_NAME"),
        google_creds_json=Path(_get_required_env("GOOGLE_CREDS_JSON")),
        scroll_pause_ms=int(os.getenv("SCROLL_PAUSE_MS", "1500")),
        max_scrolls=int(os.getenv("MAX_SCROLLS", "10")),
        output_dir=output_dir,
        csv_output_path=output_dir / "sportsplus_nba_matches.csv",
        debug_html_path=output_dir / "sportsplus_matches_debug.html",
        debug_screenshot_path=output_dir / "sportsplus_matches_debug.png",
        env_file_used=env_file_used,
    )


def _load_first_env_file() -> str | None:
    for candidate in (".env", ",env"):
        env_path = Path(candidate)
        if env_path.exists():
            load_dotenv(env_path, override=False)
            return candidate
    return None


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}
