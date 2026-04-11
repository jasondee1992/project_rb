from __future__ import annotations

import os
import logging
import sys
import time
import traceback
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from comparison_builder import (
    COMPARISON_COLUMNS,
    build_comparison_dataframe,
    build_missing_both_dataframe,
    summarize_comparison_dataframe,
)
from config import ENV_FILE_NAME, load_settings
from exceptions import ComparisonError, ScraperError, SheetsWriteError
from scraper_playtime import PlaytimeScraper
from scraper_sportsplus import SportsPlusScraper
from sheets_writer import replace_or_create_worksheet_rows, replace_worksheet_rows
from utils import FINAL_COLUMNS, dataframe_sample_text, deduplicate_dataframe, save_csv_backup


LOGGER = logging.getLogger(__name__)
SIMPLIFIED_COMPARISON_STAKE = 100.0


def main() -> int:
    _bootstrap_env()
    log_file_path = Path("logs") / "scraper.log"
    configure_logging(log_file_path)
    LOGGER.info("Script start")
    LOGGER.info("Python executable=%s", sys.executable)

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
            LOGGER.info("Cycle %s heartbeat: scraper active", cycle_number)
            try:
                settings = load_settings()
                refresh_interval_seconds = settings.refresh_interval_seconds
                LOGGER.info("Loaded HEADLESS=%s", settings.headless)
                LOGGER.info("Loaded ARB_TOTAL_STAKE=%s", settings.arb_total_stake)
                LOGGER.info("Loaded simplified NBA_COMPARISON stake=%s", SIMPLIFIED_COMPARISON_STAKE)
                LOGGER.info("Loaded MIN_GUARANTEED_PROFIT=%s", settings.min_guaranteed_profit)
                LOGGER.info("Loaded MIN_GUARANTEED_PROFIT_PERCENT=%s", settings.min_guaranteed_profit_percent)
                LOGGER.info("Loaded MAX_STAKE_PER_SIDE=%s", settings.max_stake_per_side)
                LOGGER.info("Resolved Google credentials path=%s", settings.google_creds_json)
                if settings.env_file_used:
                    LOGGER.info("Using %s configuration successfully", settings.env_file_used)
                else:
                    LOGGER.info("No %s file found; using process environment variables", ENV_FILE_NAME)
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
    combined_csv_started_at = time.time()
    save_csv_backup(combined_df, settings.combined_csv_path)
    LOGGER.info("Combined CSV export duration seconds: %.2f", time.time() - combined_csv_started_at)

    LOGGER.info("Comparison generation starting")
    try:
        comparison_df = build_comparison_dataframe(
            combined_df,
            total_stake=SIMPLIFIED_COMPARISON_STAKE,
            min_guaranteed_profit=settings.min_guaranteed_profit,
            min_guaranteed_profit_percent=settings.min_guaranteed_profit_percent,
            max_stake_per_side=settings.max_stake_per_side,
        )
    except ComparisonError:
        LOGGER.exception("Comparison generation failed")
        comparison_df = build_missing_both_dataframe(total_stake=SIMPLIFIED_COMPARISON_STAKE)
    comparison_csv_started_at = time.time()
    save_csv_backup(comparison_df, settings.comparison_csv_path, columns=COMPARISON_COLUMNS)
    LOGGER.info("Comparison CSV export duration seconds: %.2f", time.time() - comparison_csv_started_at)
    comparison_summary = summarize_comparison_dataframe(comparison_df)

    LOGGER.info("Sports Plus rows scraped: %s", len(sportsplus_df))
    LOGGER.info("PLAYTIME rows scraped: %s", len(playtime_df))
    LOGGER.info("placeholder used for PLAYTIME: %s", _playtime_placeholder_used(playtime_df))
    LOGGER.info("Combined rows after deduplication: %s", len(combined_df))
    LOGGER.info("Sample extracted rows:\n%s", dataframe_sample_text(combined_df))
    LOGGER.info("Comparison rows generated: %s", len(comparison_df))
    LOGGER.info("Comparable pairs found: %s", comparison_summary["comparable_pairs"])
    LOGGER.info("positive_arb_candidate rows found: %s", comparison_summary["positive_arb_candidates"])
    LOGGER.info("break_even_candidate rows found: %s", comparison_summary["break_even_candidates"])
    LOGGER.info("no_arb rows found: %s", comparison_summary["no_arb_rows"])
    LOGGER.info("mismatch/missing rows found: %s", comparison_summary["mismatch_missing_rows"])
    LOGGER.info("Sample comparison rows:\n%s", dataframe_sample_text(comparison_df))

    nba_rows_written = 0
    comparison_rows_written = 0
    nba_write_started_at = time.time()
    try:
        LOGGER.info("Google Sheets raw NBA worksheet write starting")
        nba_rows_written = replace_worksheet_rows(settings, combined_df)
        LOGGER.info(
            "Google Sheets raw NBA worksheet write complete: %s rows in %.2f seconds",
            nba_rows_written,
            time.time() - nba_write_started_at,
        )
    except SheetsWriteError:
        LOGGER.exception(
            "Google Sheets raw NBA worksheet write failed after %.2f seconds",
            time.time() - nba_write_started_at,
        )

    comparison_write_started_at = time.time()
    try:
        LOGGER.info("Google Sheets comparison worksheet write starting")
        comparison_rows_written = replace_or_create_worksheet_rows(
            settings=settings,
            worksheet_name=settings.comparison_worksheet_name,
            dataframe=comparison_df.reindex(columns=COMPARISON_COLUMNS).fillna(""),
            columns=COMPARISON_COLUMNS,
        )
        LOGGER.info(
            "Google Sheets comparison worksheet write complete for %s: %s rows in %.2f seconds",
            settings.comparison_worksheet_name,
            comparison_rows_written,
            time.time() - comparison_write_started_at,
        )
    except SheetsWriteError:
        LOGGER.exception(
            "Google Sheets comparison worksheet write failed after %.2f seconds",
            time.time() - comparison_write_started_at,
        )

    LOGGER.info("Cycle %s totals: SPORTS PLUS=%s PLAYTIME=%s NBA=%s NBA_COMPARISON=%s", cycle_number, len(sportsplus_df), len(playtime_df), nba_rows_written, comparison_rows_written)


def _run_source_scraper(source_name: str, action):
    started_at = time.time()
    try:
        LOGGER.info("%s scrape step starting", source_name)
        dataframe = action()
        LOGGER.info(
            "%s scrape step finished with %s rows in %.2f seconds",
            source_name,
            len(dataframe),
            time.time() - started_at,
        )
        return dataframe
    except ScraperError:
        LOGGER.exception("%s scrape step failed after %.2f seconds", source_name, time.time() - started_at)
    except Exception as error:  # noqa: BLE001
        LOGGER.exception(
            "%s scrape step failed unexpectedly after %.2f seconds: %s",
            source_name,
            time.time() - started_at,
            error,
        )
    return pd.DataFrame(columns=FINAL_COLUMNS)


def _playtime_placeholder_used(dataframe: pd.DataFrame) -> str:
    if dataframe.empty:
        return "no"
    raw_texts = dataframe.get("raw_text", pd.Series(dtype=str)).astype(str)
    return "yes" if raw_texts.str.contains("NBA not available as of this moment", na=False).any() else "no"


def configure_logging(log_file_path: Path) -> None:
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
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
    env_path = Path(ENV_FILE_NAME)
    if env_path.exists():
        load_dotenv(env_path, override=False)


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
