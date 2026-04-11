from __future__ import annotations

import os
import platform
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from utils import parse_bool


ENV_FILE_NAME = ".env"
DEFAULT_LINUX_CREDS_PATH = Path("/home/udot/PROJECTS/sports-arb-493002-6fff70e8985a.json")
DEFAULT_WINDOWS_CREDS_PATH = Path("D:/Projects/creds/sports-arb-493002-6fff70e8985a.json")


@dataclass(frozen=True)
class SourceConfig:
    name: str
    source_label: str
    source_site: str
    source_url: str
    csv_output_path: Path
    debug_html_path: Path
    debug_screenshot_path: Path


@dataclass(frozen=True)
class Settings:
    sportsplus: SourceConfig
    playtime: SourceConfig
    headless: bool
    google_sheet_name: str
    google_worksheet_name: str
    comparison_worksheet_name: str
    google_creds_json: Path
    arb_total_stake: float
    min_guaranteed_profit: float
    min_guaranteed_profit_percent: float
    max_stake_per_side: float
    scroll_pause_ms: int
    max_scrolls: int
    output_dir: Path
    combined_csv_path: Path
    comparison_csv_path: Path
    logs_dir: Path
    log_file_path: Path
    refresh_interval_seconds: int
    max_cycles: int
    run_forever: bool
    env_file_used: str | None

    @property
    def sources(self) -> tuple[SourceConfig, SourceConfig]:
        return self.sportsplus, self.playtime


def load_settings() -> Settings:
    env_file_used = _load_first_env_file()
    output_dir = Path("output")
    logs_dir = Path("logs")
    output_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    sportsplus_url = _get_required_env("SPORTSPLUS_URL")
    playtime_url = _get_required_env("PLAYTIME_URL")

    return Settings(
        sportsplus=SourceConfig(
            name="sportsplus",
            source_label=os.getenv("SPORTSPLUS_SOURCE_LABEL", "SPORTS PLUS").strip() or "SPORTS PLUS",
            source_site="sportsplus",
            source_url=sportsplus_url,
            csv_output_path=output_dir / "sportsplus_nba_matches.csv",
            debug_html_path=output_dir / "sportsplus_debug.html",
            debug_screenshot_path=output_dir / "sportsplus_debug.png",
        ),
        playtime=SourceConfig(
            name="playtime",
            source_label=os.getenv("PLAYTIME_SOURCE_LABEL", "PLAYTIME").strip() or "PLAYTIME",
            source_site="playtime",
            source_url=playtime_url,
            csv_output_path=output_dir / "playtime_nba_matches.csv",
            debug_html_path=output_dir / "playtime_debug.html",
            debug_screenshot_path=output_dir / "playtime_debug.png",
        ),
        headless=parse_bool(os.getenv("HEADLESS", "true")),
        google_sheet_name=_get_required_env("GOOGLE_SHEET_NAME"),
        google_worksheet_name=_get_required_env("GOOGLE_WORKSHEET_NAME"),
        comparison_worksheet_name=os.getenv("COMPARISON_WORKSHEET_NAME", "NBA_COMPARISON").strip() or "NBA_COMPARISON",
        google_creds_json=_resolve_google_creds_path(),
        arb_total_stake=float(os.getenv("ARB_TOTAL_STAKE", "2000").strip() or "2000"),
        min_guaranteed_profit=float(os.getenv("MIN_GUARANTEED_PROFIT", "20").strip() or "20"),
        min_guaranteed_profit_percent=float(os.getenv("MIN_GUARANTEED_PROFIT_PERCENT", "1").strip() or "1"),
        max_stake_per_side=float(os.getenv("MAX_STAKE_PER_SIDE", "1500").strip() or "1500"),
        scroll_pause_ms=int(os.getenv("SCROLL_PAUSE_MS", "1500")),
        max_scrolls=int(os.getenv("MAX_SCROLLS", "10")),
        output_dir=output_dir,
        combined_csv_path=output_dir / "all_nba_matches_combined.csv",
        comparison_csv_path=output_dir / "nba_arbitrage_comparison.csv",
        logs_dir=logs_dir,
        log_file_path=logs_dir / "scraper.log",
        refresh_interval_seconds=_get_refresh_interval_seconds(),
        max_cycles=int(os.getenv("MAX_CYCLES", "1")),
        run_forever=parse_bool(os.getenv("RUN_FOREVER", "false")),
        env_file_used=env_file_used,
    )


def _load_first_env_file() -> str | None:
    env_path = Path(ENV_FILE_NAME)
    if env_path.exists():
        load_dotenv(env_path, override=False)
        return ENV_FILE_NAME
    return None


def _get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _resolve_google_creds_path() -> Path:
    configured_value = os.getenv("GOOGLE_CREDS_JSON", "").strip()
    if configured_value:
        configured_path = Path(configured_value).expanduser()
        if configured_path.exists():
            return configured_path
        raise FileNotFoundError(
            "GOOGLE_CREDS_JSON is set but the file does not exist: "
            f"{configured_path}"
        )

    fallback_path = (
        DEFAULT_WINDOWS_CREDS_PATH
        if platform.system().lower().startswith("win")
        else DEFAULT_LINUX_CREDS_PATH
    )
    if fallback_path.exists():
        return fallback_path

    raise FileNotFoundError(
        "No valid Google credentials file found. Set GOOGLE_CREDS_JSON to an "
        "existing file or place the service-account JSON at "
        f"{fallback_path}."
    )


def _get_refresh_interval_seconds() -> int:
    configured_value = os.getenv("REFRESH_INTERVAL_SECONDS")
    if configured_value and configured_value.strip():
        return int(configured_value.strip())
    return int(os.getenv("CYCLE_SLEEP_SECONDS", "300"))
