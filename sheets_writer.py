from __future__ import annotations

import logging
import time
from typing import Iterable

import gspread
import pandas as pd

from config import DATAFRAME_COLUMNS, DEDUP_KEY_COLUMNS, Settings


LOGGER = logging.getLogger(__name__)


def upload_dataframe_to_sheet(settings: Settings, dataframe: pd.DataFrame) -> int:
    if dataframe.empty:
        LOGGER.info("No rows to upload to Google Sheets")
        return 0

    client = _with_retries(
        operation_name="Google Sheets authentication",
        action=lambda: gspread.service_account(filename=str(settings.google_creds_json)),
    )
    spreadsheet = _with_retries(
        operation_name="Spreadsheet open",
        action=lambda: client.open(settings.google_sheet_name),
    )
    worksheet = _with_retries(
        operation_name="Worksheet open",
        action=lambda: spreadsheet.worksheet(settings.google_worksheet_name),
    )

    existing_values = _with_retries(
        operation_name="Worksheet read",
        action=worksheet.get_all_values,
    )

    non_empty_rows = [row for row in existing_values if any(cell.strip() for cell in row)]

    if not non_empty_rows:
        LOGGER.info("Worksheet is empty; writing headers and %s rows", len(dataframe))
        payload = [DATAFRAME_COLUMNS] + dataframe[DATAFRAME_COLUMNS].fillna("").astype(str).values.tolist()
        _with_retries(
            operation_name="Worksheet initial write",
            action=lambda: worksheet.update(payload, value_input_option="USER_ENTERED"),
        )
        return len(dataframe)

    existing_header = non_empty_rows[0]
    if existing_header != DATAFRAME_COLUMNS:
        raise ValueError(
            "Worksheet header does not match the expected dataframe schema. "
            f"Expected {DATAFRAME_COLUMNS}, found {existing_header}."
        )

    existing_keys = {
        _build_dedup_key(dict(zip(existing_header, row, strict=False)))
        for row in non_empty_rows[1:]
    }
    LOGGER.info("Loaded %s existing dedup keys from worksheet", len(existing_keys))

    dataframe = dataframe.copy()
    dataframe["__dedup_key"] = dataframe.apply(lambda row: _build_dedup_key(row.to_dict()), axis=1)
    new_rows = dataframe.loc[~dataframe["__dedup_key"].isin(existing_keys), DATAFRAME_COLUMNS]

    if new_rows.empty:
        LOGGER.info("No new rows to append after deduplication")
        return 0

    values_to_append = new_rows.fillna("").astype(str).values.tolist()
    LOGGER.info("Appending %s new rows to Google Sheets", len(values_to_append))
    _with_retries(
        operation_name="Worksheet append",
        action=lambda: worksheet.append_rows(values_to_append, value_input_option="USER_ENTERED"),
    )
    return len(values_to_append)


def _build_dedup_key(row: dict[str, str]) -> str:
    normalized_parts = []
    for column in DEDUP_KEY_COLUMNS:
        value = str(row.get(column, "")).strip().lower()
        normalized_parts.append(value)
    return "||".join(normalized_parts)


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
