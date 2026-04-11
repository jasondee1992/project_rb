from __future__ import annotations

import logging
import sys

from config import DATAFRAME_COLUMNS, load_settings
from scraper import scrape_matches
from sheets_writer import upload_dataframe_to_sheet


LOGGER = logging.getLogger(__name__)


def main() -> int:
    _configure_logging()
    settings = load_settings()
    LOGGER.info("Loaded configuration from %s", settings.env_file_used or "environment variables")

    if not settings.google_creds_json.exists():
        raise FileNotFoundError(f"Google credentials file not found: {settings.google_creds_json}")

    dataframe = scrape_matches(settings)
    dataframe = dataframe.reindex(columns=DATAFRAME_COLUMNS).fillna("")
    dataframe.to_csv(settings.csv_output_path, index=False)
    LOGGER.info("Saved CSV backup to %s", settings.csv_output_path)

    if dataframe.empty:
        LOGGER.warning("No NBA rows were extracted; skipping Google Sheets upload")
        return 0

    sample_size = min(len(dataframe), 5)
    LOGGER.info("Sample extracted rows before upload:\n%s", dataframe.head(sample_size).to_string(index=False))

    uploaded_rows = upload_dataframe_to_sheet(settings, dataframe)
    LOGGER.info("Google Sheets upload complete; appended %s new rows", uploaded_rows)
    return 0


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


if __name__ == "__main__":
    raise SystemExit(main())
