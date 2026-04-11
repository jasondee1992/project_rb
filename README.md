# NBA Sportsbook Scraper

This project scrapes NBA betting rows from Sports Plus and Playtime with Playwright, normalizes both sources into one schema, saves CSV backups, and replaces the `Sports Betting Data` spreadsheet `NBA` worksheet with the latest scrape each run. It also rebuilds `NBA_COMPARISON` as a read-only arbitrage monitoring dashboard that stays analytical only.

## Project Structure

- `main.py`: runs both scrapers, combines rows, deduplicates, and uploads to Google Sheets
- `config.py`: loads `.env`, resolves cross-platform credentials, and builds runtime settings
- `scraper_base.py`: shared Playwright browser flow, retries, screenshots, and HTML capture
- `scraper_sportsplus.py`: Sports Plus NBA scraper
- `scraper_playtime.py`: Playtime Basketball > TODAY > NBA scraper
- `sheets_writer.py`: Google Sheets worksheet replacement logic
- `comparison_builder.py`: builds the `NBA_COMPARISON` worksheet dataset
- `utils.py`: schema constants, deduplication, CSV helpers, and text/date utilities
- `output/`: per-source CSV files, combined CSV file, and debug artifacts
- `logs/`: persistent scraper log output

## Setup

### Windows

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
playwright install
python main.py
```

### Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install
python main.py
```

## Environment Configuration

Create `.env` from `.env.example` and set the values you need:

```env
SPORTSPLUS_URL=https://www.sportsplus.ph/sbk/l/339/matches
PLAYTIME_URL=https://px4gtyp.auremi88.com/en/compact/sports/basketball
SPORTSPLUS_SOURCE_LABEL=SPORTS PLUS
PLAYTIME_SOURCE_LABEL=PLAYTIME
HEADLESS=true
GOOGLE_SHEET_NAME=Sports Betting Data
GOOGLE_WORKSHEET_NAME=NBA
COMPARISON_WORKSHEET_NAME=NBA_COMPARISON
GOOGLE_CREDS_JSON=
ARB_TOTAL_STAKE=2000
MIN_GUARANTEED_PROFIT=20
MIN_GUARANTEED_PROFIT_PERCENT=1
MAX_STAKE_PER_SIDE=1500
SCROLL_PAUSE_MS=1500
MAX_SCROLLS=10
REFRESH_INTERVAL_SECONDS=300
CYCLE_SLEEP_SECONDS=300
MAX_CYCLES=1
RUN_FOREVER=false
```

The runtime config loader uses only `.env`. `.env.example` is a template and is not loaded at runtime.

## Dynamic Google Credentials Detection

Credential resolution works like this:

1. If `GOOGLE_CREDS_JSON` is set and the file exists, that path is used.
2. If `GOOGLE_CREDS_JSON` is blank or missing, the scraper uses an OS-specific fallback path.

Fallback paths:

- Linux: `/home/udot/PROJECTS/sports-arb-493002-6fff70e8985a.json`
- Windows: `D:/Projects/creds/sports-arb-493002-6fff70e8985a.json`

If neither the environment value nor the OS fallback exists, the scraper raises a clear startup error before scraping or uploading.

## Scraping Flow

1. Load runtime settings from `.env`.
2. Launch Chromium with Playwright.
Chromium runs in headless mode by default and uses the `HEADLESS` environment variable for every browser launch.
3. Open the Sports Plus NBA list page and handle the consent modal before reading match cards.
4. Click each visible Sports Plus NBA match, scrape the detail page markets, then return to the list for the next match.
5. Scrape Playtime `Basketball > TODAY > NBA` from the rendered odds table.
6. If Playtime has no NBA league available, emit one placeholder row for that source.
7. Normalize both sources into the same final columns.
8. Save `output/sportsplus_nba_matches.csv`, `output/playtime_nba_matches.csv`, and `output/all_nba_matches_combined.csv`.
9. Deduplicate rows only within the current run.
10. Clear the `NBA` worksheet and write only the latest current dataset.
11. Build true two-outcome arbitrage comparisons from the latest raw `NBA` rows using `ARB_TOTAL_STAKE`.
12. Label the comparison output with analytical statuses such as `positive_arb_candidate`, `break_even_candidate`, `no_arb`, and mismatch or missing statuses when rows are not truly comparable.
13. Clear and rewrite `NBA_COMPARISON` with the latest dashboard output.
14. Write logs to both the console and `logs/scraper.log`.
Rows are deduplicated within the current run using:
   - `source_site`
   - `match_date`
   - `match_time`
   - `home_team`
   - `away_team`
   - `market_type`
   - `selection_name`

## Google Sheets Behavior

- Spreadsheet: `Sports Betting Data`
- Worksheet: `NBA`
- Comparison worksheet: `NBA_COMPARISON`
- Each run clears old worksheet data and writes a fresh header plus the latest current scrape results.
- Old rows from prior runs are removed instead of being preserved historically.
- If `NBA_COMPARISON` does not exist, the script creates it automatically.
- If `NBA_COMPARISON` already exists, the script clears it and rewrites the latest analytical dashboard output.
- Raw worksheet write failures and comparison worksheet write failures are logged separately.
- Every row keeps source metadata:
  - Sports Plus rows use `source_label=SPORTS PLUS`, `source_site=sportsplus`
  - Playtime rows use `source_label=PLAYTIME`, `source_site=playtime`
- If Playtime has no NBA rows at scrape time, the script writes one placeholder Playtime row with `match_status=none as of this moment`.
- Arbitrage calculations use `ARB_TOTAL_STAKE` as the total theoretical bankroll for each matched two-outcome pair and split that bankroll dynamically across both sides.
- `MIN_GUARANTEED_PROFIT`, `MIN_GUARANTEED_PROFIT_PERCENT`, and `MAX_STAKE_PER_SIDE` are optional dashboard thresholds used for labels and risk notes. They do not create direct betting commands.

## Final Output Schema

Rows are written in this exact order:

```text
source_label
source_site
source_url
scraped_at
category_name
league_name
sport_name
match_date
match_time
home_team
away_team
match_status
market_type
selection_name
odds
handicap_or_line
raw_text
```

## Troubleshooting

- If Playwright cannot start Chromium, run `playwright install chromium`.
- `HEADLESS=true` is the recommended mode for continuous background execution. Set `HEADLESS=false` only when you explicitly need a visible browser for local debugging.
- The intended virtual environment folder is `.venv`. Do not use a separate `venv/` folder.
- If the sportsbooks change their layout, inspect the latest debug HTML and PNG files in `output/`.
- Sports Plus may show a consent modal before the page is usable. The scraper now logs whether the modal appeared and saves debug artifacts before and after handling it.
- Sports Plus detail scraping now relies on the clicked match page and extracts `Handicap (Incl. Overtime)`, `Over / Under (Incl. Overtime)`, and `Winner (Incl. Overtime)` when those markets are visible.
- The arbitrage comparison CSV is written to `output/nba_arbitrage_comparison.csv` before the `NBA_COMPARISON` worksheet update.
- `NBA_COMPARISON` is informational only. It estimates candidate arbitrage metrics and stake splits but does not emit direct betting instructions or any `GO BET` style signal.
- If Playtime stops exposing the NBA table under the default Basketball page, re-check the selectors around the Basketball item, the `Today` tab, and the `Matches` market.
- If Google Sheets upload fails immediately, verify the credentials path resolution and make sure the service account still has access to the `Sports Betting Data` spreadsheet.
- If Playtime has no live NBA section, expect a single placeholder row in the sheet rather than a hard failure.
- Use `RUN_FOREVER=true` to keep the scraper cycling. `CYCLE_SLEEP_SECONDS` controls the pause between cycles, and `MAX_CYCLES` limits bounded multi-cycle runs when `RUN_FOREVER` is false.
- `REFRESH_INTERVAL_SECONDS` is the primary refresh delay for loop mode. `CYCLE_SLEEP_SECONDS` is still accepted as a compatibility fallback if `REFRESH_INTERVAL_SECONDS` is not set.
- Unexpected refresh-cycle errors are logged with full tracebacks, saved to `logs/cycle_<n>_error.txt`, then the scraper sleeps and continues to the next cycle instead of crashing the whole process.
- Press `Ctrl+C` to stop the scraper cleanly. Shutdown is logged and Playwright resources are released through the existing context managers.
- Review `logs/scraper.log` for cycle start/end times, row totals, selector lookup messages, modal handling, CSV export, and worksheet write failures.
