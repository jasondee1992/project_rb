from __future__ import annotations

import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


LOGGER = logging.getLogger(__name__)

FINAL_COLUMNS = [
    "source_label",
    "source_site",
    "source_url",
    "scraped_at",
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
    "source_site",
    "match_date",
    "match_time",
    "home_team",
    "away_team",
    "market_type",
    "selection_name",
]

DATE_TOKEN_PATTERN = re.compile(r"(?P<month>\d{1,2})/(?P<day>\d{1,2})")
TIME_TOKEN_PATTERN = re.compile(r"(?P<hour>\d{1,2}):(?P<minute>\d{2})")


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).replace("\u200e", "").replace("\u200f", "").strip()


def current_timestamp() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def with_retries(operation_name: str, action, attempts: int = 3, delay_seconds: float = 2.0):
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


def dataframe_from_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    normalized_rows = []
    for row in rows:
        normalized_row = {column: clean_text(row.get(column, "")) for column in FINAL_COLUMNS}
        normalized_rows.append(normalized_row)

    dataframe = pd.DataFrame(normalized_rows, columns=FINAL_COLUMNS).fillna("")
    return dataframe


def deduplicate_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.reindex(columns=FINAL_COLUMNS).fillna("")

    deduped = dataframe.copy()
    for column in FINAL_COLUMNS:
        if column not in deduped.columns:
            deduped[column] = ""
    deduped = deduped[FINAL_COLUMNS].fillna("")
    deduped["__dedup_key"] = deduped.apply(lambda row: build_dedup_key(row.to_dict()), axis=1)
    deduped = deduped.drop_duplicates(subset="__dedup_key").drop(columns="__dedup_key").reset_index(drop=True)
    return deduped


def build_dedup_key(row: dict[str, Any]) -> str:
    return "||".join(clean_text(row.get(column, "")).lower() for column in DEDUP_KEY_COLUMNS)


def save_csv_backup(dataframe: pd.DataFrame, path: Path, columns: list[str] | None = None) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        target_columns = columns or FINAL_COLUMNS
        dataframe.reindex(columns=target_columns).fillna("").to_csv(path, index=False)
        LOGGER.info("Saved CSV backup to %s", path)
    except Exception as error:  # noqa: BLE001
        LOGGER.exception("CSV export failed for %s: %s", path, error)
        raise


def dataframe_sample_text(dataframe: pd.DataFrame, rows: int = 5) -> str:
    if dataframe.empty:
        return "<empty dataframe>"
    sample_size = min(len(dataframe), rows)
    return dataframe.head(sample_size).to_string(index=False)


def parse_match_datetime_from_timestamp(timestamp_ms: str | int | None) -> tuple[str, str]:
    if timestamp_ms in (None, ""):
        return "", ""

    try:
        timestamp_value = int(timestamp_ms)
    except (TypeError, ValueError):
        return "", ""

    parsed = datetime.fromtimestamp(timestamp_value / 1000, tz=datetime.now().astimezone().tzinfo)
    return parsed.strftime("%Y-%m-%d"), parsed.strftime("%H:%M")


def parse_match_datetime_from_text(raw_text: str) -> tuple[str, str]:
    cleaned = clean_text(raw_text)
    if not cleaned:
        return "", ""

    date_match = DATE_TOKEN_PATTERN.search(cleaned)
    time_match = TIME_TOKEN_PATTERN.search(cleaned)
    if not (date_match and time_match):
        return "", ""

    now = datetime.now().astimezone()
    try:
        parsed = datetime(
            year=now.year,
            month=int(date_match.group("month")),
            day=int(date_match.group("day")),
            hour=int(time_match.group("hour")),
            minute=int(time_match.group("minute")),
            tzinfo=now.tzinfo,
        )
    except ValueError:
        return "", ""

    return parsed.strftime("%Y-%m-%d"), parsed.strftime("%H:%M")
