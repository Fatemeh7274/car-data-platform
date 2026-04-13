import os
import requests
import json
from datetime import datetime
from pathlib import Path
import logging


# Logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Config

BASE_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/getallmanufacturers"

METADATA_FILE = Path("metadata/nhtsa_last_ingestion.json")


# Partition

def get_current_partition():
    now = datetime.utcnow()
    return {
        "year": now.strftime("%Y"),
        "month": now.strftime("%m"),
        "day": now.strftime("%d"),
        "date_str": now.strftime("%Y%m%d")
    }


# Metadata State

def get_last_ingestion_date():
    if not METADATA_FILE.exists():
        return None

    with open(METADATA_FILE, "r") as f:
        metadata = json.load(f)

    return metadata.get("last_ingestion_date")


def save_ingestion_metadata(record_count):

    metadata_dir = Path("data/metadata")
    metadata_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "last_ingestion_date": datetime.utcnow().isoformat(),
        "record_count": record_count
    }

    metadata_file = metadata_dir / "nhtsa_last_ingestion.json"

    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)


# Fetch API Data

def fetch_data():
    params = {
        "format": "json"
    }

    headers = {
        "User-Agent": "car-data-platform/1.0",
        "Accept": "application/json"
    }

    response = requests.get(
        BASE_URL,
        params=params,
        headers=headers,
        timeout=30
    )

    response.raise_for_status()
    return response.json()


# Storage

def build_output_path(partition):
    base_path = Path("data/raw/nhtsa")

    full_path = (
        base_path /
        f"year={partition['year']}" /
        f"month={partition['month']}" /
        f"day={partition['day']}"
    )

    full_path.mkdir(parents=True, exist_ok=True)
    return full_path


def save_data(data, output_path, date_str):

    file_path = output_path / f"manufacturers_{date_str}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    return file_path


def save_metadata(output_path, data_file_path, record_count):

    metadata = {
        "source": "nhtsa_manufacturers_api",
        "ingestion_timestamp_utc": datetime.utcnow().isoformat(),
        "file_name": data_file_path.name,
        "file_size_bytes": data_file_path.stat().st_size,
        "record_count": record_count
    }

    metadata_path = output_path / f"{data_file_path.stem}.metadata.json"

    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    return metadata_path


# Main Pipeline

def main():

    logger.info("Starting NHTSA ingestion...")

    # Incremental state
    last_ingestion = get_last_ingestion_date()
    logger.info(f"Last ingestion time: {last_ingestion}")

    data = fetch_data()

    records = data.get("Results", [])
    record_count = len(records)

    logger.info(f"Records received: {record_count}")

    partition = get_current_partition()

    output_path = build_output_path(partition)

    file_path = save_data(
        data,
        output_path,
        partition["date_str"]
    )

    metadata_path = save_metadata(
        output_path,
        file_path,
        record_count
    )

    save_ingestion_metadata(record_count)

    logger.info(f"Raw data saved at: {file_path}")
    logger.info(f"Metadata saved at: {metadata_path}")

    logger.info("NHTSA ingestion completed successfully.")


if __name__ == "__main__":
    main()