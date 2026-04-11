from __future__ import annotations

import os
import logging
import sys
import time
import traceback
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from comparison_builder import COMPARISON_COLUMNS, build_comparison_dataframe, build_missing_both_dataframe
from config import load_settings
from exceptions import ComparisonError, ScraperError, SheetsWriteError
from scraper_playtime import PlaytimeScraper
from scraper_sportsplus import SportsPlusScraper
from sheets_writer import replace_or_create_worksheet_rows, replace_worksheet_rows
from utils import FINAL_COLUMNS, dataframe_sample_text, deduplicate_dataframe, save_csv_backup


LOGGER = logging.getLogger(__name__)


def main() -> int:
    _bootstrap_env()
    log_file_path = Path("logs") / "scraper.log"
    configure_logging(log_file_path)

    run_forever = _parse_bool_env("RUN_FOREVER", default=False)
    max_cycles = int(os.getenv("MAX_CYCLES", "1"))
    refresh_interval_seconds = _resolve_refresh_interval_seconds()
    cycle_number = 0
    last_exit_code = 0

    try:
        while True:
            cycle_number += 1
            cycle_started_at = time.time()
            LOGGER.info("Cycle %s start time: %s", cycle_number, time.strftime("%Y-%m-%d %H:%M:%S%z"))
            try:
                settings = load_settings()
                refresh_interval_seconds = settings.refresh_interval_seconds
                LOGGER.info("Loaded configuration from %s", settings.env_file_used or "environment variables")
                LOGGER.info("Using Google credentials file %s", settings.google_creds_json)
                _run_cycle(cycle_number, settings)
                last_exit_code = 0
            except KeyboardInterrupt:
                raise
            except Exception as error:  # noqa: BLE001
                LOGGER.exception("Cycle %s failed unexpectedly: %s", cycle_number, error)
                _save_cycle_error_artifact(cycle_number, error)
                last_exit_code = 1

            duration_seconds = time.time() - cycle_started_at
            LOGGER.info("Cycle %s end time: %s", cycle_number, time.strftime("%Y-%m-%d %H:%M:%S%z"))
            LOGGER.info("Cycle %s duration seconds: %.2f", cycle_number, duration_seconds)

            if not run_forever and cycle_number >= max_cycles:
                break

            LOGGER.info("Sleeping %s seconds before next cycle", refresh_interval_seconds)
            time.sleep(refresh_interval_seconds)
    except KeyboardInterrupt:
        LOGGER.info("Scraper stopped manually via KeyboardInterrupt")
        return 130

    return last_exit_code


def _run_cycle(cycle_number: int, settings) -> None:
    LOGGER.info("Cycle %s source scraping starting", cycle_number)
    sportsplus_df = _run_source_scraper("SPORTS PLUS", lambda: SportsPlusScraper(settings, settings.sportsplus).run())
    playtime_df = _run_source_scraper("PLAYTIME", lambda: PlaytimeScraper(settings, settings.playtime).run())

    combined_df = pd.concat([sportsplus_df, playtime_df], ignore_index=True)
    combined_df = deduplicate_dataframe(combined_df.reindex(columns=FINAL_COLUMNS).fillna(""))
    save_csv_backup(combined_df, settings.combined_csv_path)

    LOGGER.info("Comparison generation starting")
    try:
        comparison_df = build_comparison_dataframe(combined_df)
    except ComparisonError:
        LOGGER.exception("Comparison generation failed")
        comparison_df = build_missing_both_dataframe()
    save_csv_backup(comparison_df, settings.comparison_csv_path, columns=COMPARISON_COLUMNS)

    LOGGER.info("Sports Plus rows scraped: %s", len(sportsplus_df))
    LOGGER.info("PLAYTIME rows scraped: %s", len(playtime_df))
    LOGGER.info("placeholder used for PLAYTIME: %s", _playtime_placeholder_used(playtime_df))
    LOGGER.info("Combined rows after deduplication: %s", len(combined_df))
    LOGGER.info("Sample extracted rows:\n%s", dataframe_sample_text(combined_df))
    LOGGER.info("Comparison rows generated: %s", len(comparison_df))
    LOGGER.info("Sample comparison rows:\n%s", dataframe_sample_text(comparison_df))

    nba_rows_written = 0
    comparison_rows_written = 0
    try:
        LOGGER.info("Google Sheets raw NBA worksheet write starting")
        nba_rows_written = replace_worksheet_rows(settings, combined_df)
        LOGGER.info("Google Sheets raw NBA worksheet write complete: %s rows", nba_rows_written)
    except SheetsWriteError:
        LOGGER.exception("Google Sheets raw NBA worksheet write failed")

    try:
        LOGGER.info("Google Sheets comparison worksheet write starting")
        comparison_rows_written = replace_or_create_worksheet_rows(
            settings=settings,
            worksheet_name=settings.comparison_worksheet_name,
            dataframe=comparison_df.reindex(columns=COMPARISON_COLUMNS).fillna(""),
            columns=COMPARISON_COLUMNS,
        )
        LOGGER.info(
            "Google Sheets comparison worksheet write complete for %s: %s rows",
            settings.comparison_worksheet_name,
            comparison_rows_written,
        )
    except SheetsWriteError:
        LOGGER.exception("Google Sheets comparison worksheet write failed")

    LOGGER.info("Cycle %s totals: SPORTS PLUS=%s PLAYTIME=%s NBA=%s NBA_COMPARISON=%s", cycle_number, len(sportsplus_df), len(playtime_df), nba_rows_written, comparison_rows_written)


def _run_source_scraper(source_name: str, action):
    try:
        LOGGER.info("%s scrape step starting", source_name)
        dataframe = action()
        LOGGER.info("%s scrape step finished with %s rows", source_name, len(dataframe))
        return dataframe
    except ScraperError:
        LOGGER.exception("%s scrape step failed", source_name)
    except Exception as error:  # noqa: BLE001
        LOGGER.exception("%s scrape step failed unexpectedly: %s", source_name, error)
    return pd.DataFrame(columns=FINAL_COLUMNS)


def _playtime_placeholder_used(dataframe: pd.DataFrame) -> str:
    if dataframe.empty:
        return "no"
    raw_texts = dataframe.get("raw_text", pd.Series(dtype=str)).astype(str)
    return "yes" if raw_texts.str.contains("NBA not available as of this moment", na=False).any() else "no"


def configure_logging(log_file_path: Path) -> None:
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logging.basicConfig(
        level=logging.INFO,
        handlers=[console_handler, file_handler],
        force=True,
    )


def _bootstrap_env() -> None:
    for candidate in (".env", ",env"):
        env_path = Path(candidate)
        if env_path.exists():
            load_dotenv(env_path, override=False)
            return


def _parse_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _resolve_refresh_interval_seconds() -> int:
    refresh_value = os.getenv("REFRESH_INTERVAL_SECONDS")
    if refresh_value and refresh_value.strip():
        return int(refresh_value.strip())
    return int(os.getenv("CYCLE_SLEEP_SECONDS", "300"))


def _save_cycle_error_artifact(cycle_number: int, error: Exception) -> None:
    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = logs_dir / f"cycle_{cycle_number}_error.txt"
    artifact_path.write_text(
        "".join(traceback.format_exception(type(error), error, error.__traceback__)),
        encoding="utf-8",
    )
    LOGGER.info("Saved cycle error artifact to %s", artifact_path)


if __name__ == "__main__":
    raise SystemExit(main())
