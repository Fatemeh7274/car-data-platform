import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import datetime
import json
from time import sleep


# CONFIG

DB_URI = os.getenv(
    "POSTGRES_URI",
    "postgresql://postgres:password@localhost:5432/car_data"
)

BASE_PARQUET_PATH = Path("data/staging/sales")
TABLE_NAME = "stg_sales"

MAX_RETRIES = 3
RETRY_DELAY = 5

METADATA_FILE = Path("data/metadata/stg_sales_load_metadata.json")



# LOGGING

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("sales_loader")



# METADATA

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

    METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(METADATA_FILE, "w") as f:
        json.dump(history, f, indent=2)



# DISCOVER FILES

def discover_parquet():
    return sorted(BASE_PARQUET_PATH.glob("year=*/month=*/*.parquet"))



# EXTRACT PARTITION

def extract_partition(path: Path):

    year, month = None, None

    for p in path.parts:
        if p.startswith("year="):
            year = p.split("=")[1]
        if p.startswith("month="):
            month = p.split("=")[1]

    return year, month



# LOAD ONE FILE

def load_file(parquet_file: Path, engine):

    year, month = extract_partition(parquet_file)

    logger.info(f"Processing {year}-{month}")

    df = pd.read_parquet(parquet_file)

    if df.empty:
        logger.warning("Empty file skipped")
        return 0

    retries = 0

    while retries < MAX_RETRIES:

        try:

            # FETCH EXISTING DATA

            query = text(f"""
                SELECT sale_date, vehicle_make, vehicle_model
                FROM {TABLE_NAME}
                WHERE year = :year AND month = :month
            """)

            existing = pd.read_sql(
                query,
                engine,
                params={"year": year, "month": month}
            )


            # REMOVE DUPLICATES

            if not existing.empty:

                df_new = df.merge(
                    existing,
                    on=["sale_date", "vehicle_make", "vehicle_model"],
                    how="left",
                    indicator=True
                )

                df_new = df_new[df_new["_merge"] == "left_only"].drop(columns="_merge")

            else:
                df_new = df

            if df_new.empty:
                logger.info("No new rows to insert")
                return 0


            # INSERT

            df_new.to_sql(
                TABLE_NAME,
                engine,
                if_exists="append",
                index=False,
                method="multi"
            )

            rows = len(df_new)

            logger.info(f"Inserted {rows} rows")

            save_metadata(year, month, rows)

            return rows

        except Exception as e:

            retries += 1

            logger.warning(f"Retry {retries}/{MAX_RETRIES}")
            logger.warning(str(e))

            sleep(RETRY_DELAY)

    logger.error(f"FAILED loading {year}-{month}")

    return 0



# MAIN

def load_sales_staging():

    logger.info("Starting Sales Load Pipeline")

    files = discover_parquet()

    if not files:
        logger.error(f"No parquet files found in {BASE_PARQUET_PATH}")
        return

    logger.info(f"{len(files)} partitions discovered")

    engine = create_engine(DB_URI)

    total_rows = 0

    for file in files:
        total_rows += load_file(file, engine)

    logger.info(f"Total rows loaded: {total_rows}")
    logger.info("Sales Load Pipeline Finished")



# ENTRY

if __name__ == "__main__":
    load_sales_staging()