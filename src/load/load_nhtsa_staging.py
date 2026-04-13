import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
import logging
import os

# -----------------------
# Config
# -----------------------
STAGING_PATH = Path("data/staging/nhtsa/nhtsa_data")
DB_URI = os.getenv("POSTGRES_URI", "postgresql://postgres:password@localhost:5432/car_data")
TABLE_NAME = "nhtsa"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load Parquet Files

def load_parquet_files():

    files = list(STAGING_PATH.rglob("*.parquet"))

    if not files:
        logging.warning("No Parquet files found to load")
        return None

    logging.info(f"Found {len(files)} parquet files")

    dfs = []

    for f in files:
        try:
            df = pd.read_parquet(f, engine="pyarrow")
            dfs.append(df)
        except Exception as e:
            logging.error(f"Failed to read {f}: {e}")

    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
        logging.info(f"Total rows loaded from parquet: {len(final_df)}")
        return final_df
    else:
        logging.warning("No valid dataframes read from parquet")
        return None

# Load to PostgreSQL
def load_to_postgres(df):

    if df is None or df.empty:
        logging.warning("No data to load into PostgreSQL")
        return

    try:
        engine = create_engine(DB_URI)
        df.to_sql(
            TABLE_NAME,
            engine,
            if_exists="append",  
            index=False,
            method="multi"
        )
        logging.info(f"Data successfully loaded to table '{TABLE_NAME}'")
    except Exception as e:
        logging.error(f"Failed to load data to PostgreSQL: {e}")

# Main
def main():

    logging.info("Starting NHTSA load process")

    df = load_parquet_files()
    load_to_postgres(df)

    logging.info("NHTSA load process finished")


if __name__ == "__main__":
    main()