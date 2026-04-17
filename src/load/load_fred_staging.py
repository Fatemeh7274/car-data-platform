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

BASE_PATH = Path(r"C:\projects\car-data-platform\data\staging\fred")

TABLE_NAME = "stg_fred"

MAX_RETRIES = 3
RETRY_DELAY = 5

METADATA_FILE = Path("data/metadata/stg_fred_load_metadata.json")


# LOGGING

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("fred_loader")


# DISCOVER FILES

def discover_files():
    return sorted(BASE_PATH.glob("series_id=*/year=*/month=*/*.parquet"))


# EXTRACT PARTITION INFO

def extract_info(path: Path):

    series_id, year, month = None, None, None

    for p in path.parts:
        if p.startswith("series_id="):
            series_id = p.split("=")[1]
        if p.startswith("year="):
            year = p.split("=")[1]
        if p.startswith("month="):
            month = p.split("=")[1]

    return series_id, year, month


# METADATA

def save_metadata(series_id, year, month, rows):

    record = {
        "table": TABLE_NAME,
        "series_id": series_id,
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



# LOAD PARTITION

def load_partition(file: Path, engine):

    series_id, year, month = extract_info(file)

    logger.info(f"Processing {series_id} | {year}-{month}")

    df = pd.read_parquet(file)

    if df.empty:
        logger.warning("Empty parquet file")
        return 0

    retries = 0

    while retries < MAX_RETRIES:

        try:
            # ======================
            # FETCH EXISTING DATA
            # ======================

            query = text(f"""
                SELECT observation_date
                FROM {TABLE_NAME}
                WHERE series_id = :series_id
                AND observation_date >= :start_date
            """)

            existing = pd.read_sql(
                query,
                engine,
                params={
                    "series_id": series_id,
                    "start_date": f"{year}-{month}-01"
                }
            )

            # ======================
            # FILTER NEW DATA
            # ======================

            if not existing.empty:
                df_new = df[
                    ~df["observation_date"].isin(existing["observation_date"])
                ]
            else:
                df_new = df

            if df_new.empty:
                logger.info("No new data to insert")
                return 0

            # ======================
            # INSERT DATA
            # ======================

            df_new.to_sql(
                TABLE_NAME,
                engine,
                if_exists="append",
                index=False,
                method="multi"
            )

            rows = len(df_new)

            logger.info(f"Inserted {rows} rows")

            save_metadata(series_id, year, month, rows)

            return rows

        except Exception as e:

            retries += 1

            logger.warning(f"Retry {retries}/{MAX_RETRIES}")
            logger.warning(str(e))

            sleep(RETRY_DELAY)

    logger.error(f"FAILED partition {series_id} {year}-{month}")

    return 0


# MAIN

def load_fred_staging():

    logger.info("Starting FRED Load Pipeline")

    files = discover_files()

    if not files:
        logger.error("No parquet files found")
        return

    logger.info(f"{len(files)} partitions found")

    engine = create_engine(DB_URI)

    total_rows = 0

    for f in files:
        total_rows += load_partition(f, engine)

    logger.info(f"Total rows loaded: {total_rows}")


# ENTRY POINT

if __name__ == "__main__":
    load_fred_staging()