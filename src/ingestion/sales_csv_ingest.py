import shutil
import json
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


SOURCE_FILE = Path("external_data/sales/used_cars.csv")


def get_current_partition():
    now = datetime.utcnow()
    return {
        "year": now.strftime("%Y"),
        "month": now.strftime("%m"),
        "day": now.strftime("%d"),
        "date_str": now.strftime("%Y%m%d")
    }


def build_output_path(partition):
    base_path = Path("data/raw/sales")
    full_path = base_path / f"year={partition['year']}" / f"month={partition['month']}" / f"day={partition['day']}"
    full_path.mkdir(parents=True, exist_ok=True)
    return full_path


def save_metadata(output_path, data_file_path, record_count):
    metadata = {
        "source": "used_cars_csv",
        "ingestion_timestamp_utc": datetime.utcnow().isoformat(),
        "file_name": data_file_path.name,
        "file_size_bytes": data_file_path.stat().st_size,
        "record_count": record_count
    }

    metadata_path = output_path / f"{data_file_path.stem}.metadata.json"

    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    return metadata_path


def main():
    logger.info("Starting Sales CSV ingestion...")

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")

    # Count records
    df = pd.read_csv(SOURCE_FILE)
    record_count = len(df)
    logger.info(f"Records found in CSV: {record_count}")

    partition = get_current_partition()
    output_path = build_output_path(partition)

    destination_file = output_path / f"sales_{partition['date_str']}.csv"

    shutil.copy2(SOURCE_FILE, destination_file)
    logger.info(f"File copied to raw: {destination_file}")

    metadata_path = save_metadata(output_path, destination_file, record_count)
    logger.info(f"Metadata saved at: {metadata_path}")

    logger.info("Sales CSV ingestion completed successfully.")


if __name__ == "__main__":
    main()