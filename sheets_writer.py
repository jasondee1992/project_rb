from __future__ import annotations

import logging

import gspread
import pandas as pd
from gspread.exceptions import WorksheetNotFound

from config import Settings
from exceptions import SheetsWriteError
from utils import FINAL_COLUMNS, with_retries


LOGGER = logging.getLogger(__name__)


def replace_worksheet_rows(settings: Settings, dataframe: pd.DataFrame) -> int:
    return replace_or_create_worksheet_rows(
        settings=settings,
        worksheet_name=settings.google_worksheet_name,
        dataframe=dataframe,
        columns=FINAL_COLUMNS,
    )


def replace_or_create_worksheet_rows(
    *,
    settings: Settings,
    worksheet_name: str,
    dataframe: pd.DataFrame,
    columns: list[str],
) -> int:
    try:
        client = with_retries(
            operation_name="Google Sheets authentication",
            action=lambda: gspread.service_account(filename=str(settings.google_creds_json)),
        )
        spreadsheet = with_retries(
            operation_name="Open spreadsheet",
            action=lambda: client.open(settings.google_sheet_name),
        )
        worksheet = _get_or_create_worksheet(
            spreadsheet=spreadsheet,
            worksheet_name=worksheet_name,
            columns=len(columns),
        )

        existing_values = with_retries(
            operation_name="Read worksheet values",
            action=worksheet.get_all_values,
        )
        populated_rows = [row for row in existing_values if any(str(cell).strip() for cell in row)]
        if populated_rows:
            LOGGER.info("Clearing %s existing worksheet rows before writing %s", len(populated_rows), worksheet_name)
        else:
            LOGGER.info("Worksheet %s is currently empty; writing a fresh header and dataset", worksheet_name)

        payload = [columns]
        if dataframe.empty:
            LOGGER.info("No current rows available for worksheet %s; clearing stale data and writing headers only", worksheet_name)
        else:
            payload.extend(dataframe[columns].fillna("").astype(str).values.tolist())
        with_retries(
            operation_name="Clear worksheet",
            action=worksheet.clear,
        )
        with_retries(
            operation_name="Write fresh worksheet data",
            action=lambda: worksheet.update("A1", payload, value_input_option="USER_ENTERED"),
        )
        LOGGER.info("Replaced worksheet %s contents with %s current data rows", worksheet_name, len(dataframe))
        return len(dataframe)
    except Exception as error:  # noqa: BLE001
        LOGGER.exception("Google Sheets write failed for worksheet %s: %s", worksheet_name, error)
        raise SheetsWriteError(f"Failed writing worksheet {worksheet_name}") from error


def _get_or_create_worksheet(*, spreadsheet, worksheet_name: str, columns: int):
    try:
        return with_retries(
            operation_name=f"Open worksheet {worksheet_name}",
            action=lambda: spreadsheet.worksheet(worksheet_name),
        )
    except WorksheetNotFound:
        LOGGER.info("Worksheet %s does not exist; creating it", worksheet_name)
        return with_retries(
            operation_name=f"Create worksheet {worksheet_name}",
            action=lambda: spreadsheet.add_worksheet(title=worksheet_name, rows=100, cols=max(columns, 20)),
        )
