import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
import logging
import os
from time import sleep


# Config

STAGING_PATH = Path("data/staging/nhtsa/nhtsa_data")
DB_URI = os.getenv("POSTGRES_URI", "postgresql://postgres:password@localhost:5432/car_data")
TABLE_NAME = "stg_nhtsa"

MAX_RETRIES = 3
RETRY_DELAY = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("nhtsa_loader")


# Discover Files

def discover_files():
    files = sorted(STAGING_PATH.rglob("*.parquet"))

    if not files:
        logger.warning("No parquet files found")

    else:
        logger.info(f"Found {len(files)} parquet files")

    return files



# Extract Partition Info

def extract_info(path: Path):

    year = None
    ingestion_date = None

    for p in path.parts:
        if p.startswith("year="):
            year = p.split("=")[1]
        if p.startswith("ingestion_date="):
            ingestion_date = p.split("=")[1]

    return year, ingestion_date



# Load Partition

def load_partition(file, engine):

    year, ingestion_date = extract_info(file)

    logger.info(f"Processing year={year}, ingestion_date={ingestion_date}")

    df = pd.read_parquet(file)

    if df.empty:
        return 0

    retries = 0

    while retries < MAX_RETRIES:

        try:
            # check existing data
            query = text(f"""
                SELECT vehicle_id, recall_id
                FROM {TABLE_NAME}
                WHERE year = :year
            """)

            existing = pd.read_sql(query, engine, params={"year": year})

            if not existing.empty:
                df_new = df.merge(
                    existing,
                    on=["vehicle_id", "recall_id"],
                    how="left",
                    indicator=True
                )

                df_new = df_new[df_new["_merge"] == "left_only"].drop(columns="_merge")

            else:
                df_new = df

            if df_new.empty:
                logger.info("No new rows to insert")
                return 0

            df_new.to_sql(
                TABLE_NAME,
                engine,
                if_exists="append",
                index=False,
                method="multi"
            )

            rows = len(df_new)
            logger.info(f"Inserted {rows} rows")

            return rows

        except Exception as e:
            retries += 1
            logger.warning(f"Retry {retries}/{MAX_RETRIES}")
            logger.warning(str(e))
            sleep(RETRY_DELAY)

    logger.error(f"FAILED partition {year}-{ingestion_date}")
    return 0


# Main

def load_nhtsa_staging():

    logger.info("Starting NHTSA load pipeline")

    files = discover_files()

    if not files:
        return

    engine = create_engine(DB_URI)

    total = 0

    for f in files:
        total += load_partition(f, engine)

    logger.info(f"Total rows loaded: {total}")


# Entry

if __name__ == "__main__":
    load_nhtsa_staging()