# SportsPlus NBA Scraper

Python scraper for the rendered SportsPlus NBA matches page. It uses Playwright for browser rendering, normalizes visible NBA match rows into a pandas dataframe, saves a CSV backup, and appends only new rows into Google Sheets.

## Project Structure

- `main.py`: entry point and orchestration
- `config.py`: environment loading and runtime settings
- `scraper.py`: Playwright browser automation and DOM extraction
- `sheets_writer.py`: Google Sheets authentication, deduplication, and append logic
- `requirements.txt`: Python dependencies
- `.env.example`: required environment variables
- `output/`: CSV backup and debug artifacts

## Setup

### 1. Create a virtual environment

Windows:

```powershell
python -m venv .venv
.venv\Scripts\activate
```

### 2. Install dependencies

```powershell
pip install -r requirements.txt
```

### 3. Install Playwright browsers

```powershell
playwright install
```

## Environment Configuration

Create a `.env` file in the project root using `.env.example` as a template.

Expected values for this project:

```env
TARGET_URL=https://www.sportsplus.ph/sbk/l/339/matches
HEADLESS=false
GOOGLE_SHEET_NAME=Sports Betting Data
GOOGLE_WORKSHEET_NAME=NBA
GOOGLE_CREDS_JSON=D:/Projects/creds/sports-arb-493002-6fff70e8985a.json
SCROLL_PAUSE_MS=1500
MAX_SCROLLS=10
```

Note: the loader also falls back to `,env` if `.env` is not present, because that filename exists in this workspace.

## Run

Windows:

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
playwright install
python main.py
```

## How It Works

1. Opens the target page in Chromium with Playwright.
2. Waits for the rendered page to settle.
3. Attempts to dismiss the terms overlay if it blocks interaction.
4. Scrolls the page to trigger lazy-loaded content.
5. Extracts visible `.s7k-match-box` cards from the live DOM.
6. Normalizes the visible market selections into dataframe rows.
7. Filters the final dataset to NBA rows.
8. Saves the dataframe to `output/sportsplus_nba_matches.csv`.
9. Appends only new rows to the `Sports Betting Data` spreadsheet, `NBA` worksheet.

## Google Sheets Behavior

- Authentication uses the service account JSON path from `GOOGLE_CREDS_JSON`.
- If the worksheet is empty, the scraper writes the header row first, followed by all extracted rows.
- If rows already exist, it reads the current sheet and builds a deduplication key using:
  - `match_date`
  - `match_time`
  - `home_team`
  - `away_team`
  - `market_type`
  - `selection_name`
- Only rows with unseen keys are appended.

## Output Files

- CSV backup: `output/sportsplus_nba_matches.csv`
- Debug HTML: `output/sportsplus_matches_debug.html`
- Debug screenshot: `output/sportsplus_matches_debug.png`

## Troubleshooting

- If Playwright fails to launch, run `playwright install chromium`.
- If `pip install` fails with a certificate bundle error on this machine, pass the local certifi bundle explicitly:

```powershell
python -m pip install -r requirements.txt --cert "C:\Users\JasonD\AppData\Local\Programs\Python\Python311\Lib\site-packages\certifi\cacert.pem"
```

- If the page layout changes, inspect `output/sportsplus_matches_debug.html` and update the selectors in `scraper.py`.
- If the browser window opens but scraping stalls, set `HEADLESS=true` temporarily to compare behavior.
- If the worksheet headers were created manually and do not match the expected schema exactly, the script will stop with a header mismatch error instead of appending to the wrong columns.
- If no rows are uploaded, check the logs for selector counts and deduplication results.
