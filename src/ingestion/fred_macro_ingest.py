import os
import json
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv

# -----------------------
# Config
# -----------------------
METADATA_FILE = "data/metadata/fred_ingestion_state.json"
load_dotenv()

API_KEY = os.getenv("FRED_API_KEY")

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

SERIES = {
    "unemployment_rate": "UNRATE",
    "cpi": "CPIAUCSL",
    "interest_rate": "FEDFUNDS",
    "gdp": "GDP",
    "consumer_sentiment": "UMCSENT"
}

RAW_DIR = "data/raw/fred"

# -----------------------
# Logging
# -----------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------
# Fetch Data
# -----------------------

def fetch_series(series_id, last_date=None):

    params = {
        "series_id": series_id,
        "api_key": API_KEY,
        "file_type": "json"
    }

    # Incremental ingestion
    if last_date:
        params["observation_start"] = last_date

    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


# -----------------------
# Save Raw JSON (Immutable)
# -----------------------

def save_raw(series_id, data):

    timestamp = datetime.utcnow().strftime("%Y-%m-%d")

    path = os.path.join(RAW_DIR, f"series_id={series_id}")
    os.makedirs(path, exist_ok=True)

    file_path = os.path.join(path, f"{timestamp}.json")

    with open(file_path, "w") as f:
        json.dump(data, f)

    return file_path

# ----------------------
# Metadate
# ---------------------
def save_metadata(series_id, raw_json, status="SUCCESS"):

    os.makedirs("data/metadata", exist_ok=True)

    observations = raw_json.get("observations", [])

    dates = [obs["date"] for obs in observations if obs.get("date")]

    metadata = {
        "series_id": series_id,
        "ingestion_timestamp_utc": datetime.utcnow().isoformat(),
        "record_count": len(observations),
        "min_date": min(dates) if dates else None,
        "max_date": max(dates) if dates else None,
        "status": status
    }

    # Append style metadata storage
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            history = json.load(f)
    else:
        history = []

    history.append(metadata)

    with open(METADATA_FILE, "w") as f:
        json.dump(history, f, indent=2)


# -----------------------
# Main Pipeline
# -----------------------

def main():

    for name, series_id in SERIES.items():

        try:
            logging.info(f"Fetching {series_id}")

            raw_json = fetch_series(series_id)

            if not raw_json.get("observations"):
                logging.info(f"No new data for {series_id}")
                save_metadata(series_id, raw_json, "NO_NEW_DATA")
                continue

            # Save raw data
            raw_path = save_raw(series_id, raw_json)

            # Save metadata
            save_metadata(series_id, raw_json, "SUCCESS")

            logging.info(f"{series_id} saved to {raw_path}")

        except Exception as e:

            logging.error(f"Failed ingestion for {series_id}: {e}")

            # Save failure metadata
            save_metadata(series_id, {}, "FAILED")


if __name__ == "__main__":
    main()