import os
import logging
import pandas as pd
from sqlalchemy import create_engine, inspect
from pathlib import Path
from datetime import datetime
import json
from time import sleep


# CONFIG

DB_URI = os.getenv("POSTGRES_URI","postgresql://postgres:password@localhost:5432/car_data")
BASE_PARQUET_PATH = Path("data\staging\sales")
TABLE_NAME = "stg_sales"
MAX_RETRIES = 3
RETRY_DELAY = 5

# metadata storage
METADATA_DIR = Path("data/metadata")
METADATA_DIR.mkdir(parents=True, exist_ok=True)

METADATA_FILE = METADATA_DIR / "stg_sales_load_metadata.json"


# LOGGING

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s")

logger = logging.getLogger("sales_loader")


# METADATA SAVE

def save_metadata(year, month, rows):

    record = {
        "table": TABLE_NAME,
        "year": year,
        "month": month,
        "rows_loaded": rows,
        "timestamp": datetime.utcnow().isoformat()
    }

    if METADATA_FILE.exists():

        with open(METADATA_FILE, "r") as f:
            history = json.load(f)
        
    else:
        history = []

    history.append(record)

    with open(METADATA_FILE, "w") as f:
        json.dump(history, f, indent=2)


# PARTITION INFO

def extract_partition(path: Path):

    parts = path.parts
    year = None
    month = None

    for p in parts:

        if p.startswith("year="):
            year = p.split("=")[1]

        if p.startswith("month="):
            month = p.split("=")[1]

    return year, month


# LOAD ONE FILE

def load_file(parquet_file: Path, engine):

    year, month = extract_partition(parquet_file)

    logger.info(f"Processing {parquet_file}")

    df = pd.read_parquet(parquet_file)

    if df.empty:
        logger.info("Empty file skipped")
        return 0

    retries = 0

    while retries < MAX_RETRIES:

        try:

            df.to_sql(
                TABLE_NAME,
                engine,
                if_exists="append",
                index=False
            )

            rows = len(df)

            logger.info(
                f"Loaded {rows} rows for partition {year}-{month}"
            )

            save_metadata(year, month, rows)

            return rows

        except Exception as e:

            retries += 1

            logger.warning(
                f"Retry {retries}/{MAX_RETRIES} for {parquet_file}"
            )

            logger.warning(str(e))

            sleep(RETRY_DELAY)

    logger.error(f"FAILED loading {parquet_file}")

    return 0


# DISCOVER FILES

def discover_parquet():

    parquet_files = sorted(
        BASE_PARQUET_PATH.glob("year=*/month=*/*.parquet"))

    return parquet_files


# MAIN PIPELINE

def main():

    logger.info("Starting Sales Load Pipeline")

    parquet_files = discover_parquet()

    if not parquet_files:

        logger.error(
            f"No parquet files found in {BASE_PARQUET_PATH}"
        )

        return

    logger.info(f"{len(parquet_files)} parquet files discovered")

    engine = create_engine(DB_URI)

    total_rows = 0

    for file in parquet_files:

        rows = load_file(file, engine)

        total_rows += rows

    logger.info(f"Total rows loaded: {total_rows}")

    logger.info("Sales Load Pipeline Finished")



if __name__ == "__main__":
    main()