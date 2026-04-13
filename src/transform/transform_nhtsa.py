import pandas as pd
from pathlib import Path
import json
import logging
from datetime import datetime

# Paths
RAW_PATH = Path("data/raw/api/nhtsa")
PROCESSED_PATH = Path("data/staging/nhtsa")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# Get Raw Files

def get_raw_files():

    files = [
        f for f in RAW_PATH.rglob("*.json")
        if not f.name.endswith(".metadata.json")
    ]

    if not files:
        logging.warning("No NHTSA raw files found")
    else:
        logging.info(f"Found {len(files)} raw files")

    return files



# Read JSON

def read_json(file):

    try:
        with open(file) as f:
            raw = json.load(f)
        return raw

    except Exception as e:
        logging.error(f"Failed reading {file}: {e}")
        return None


# Flatten JSON

def flatten_json(raw):

    try:
        if isinstance(raw, dict):
            df = pd.json_normalize(raw.get("results", raw))
        else:
            df = pd.json_normalize(raw)

        return df

    except Exception as e:
        logging.error(f"JSON flatten failed: {e}")
        return pd.DataFrame()



# Clean Data

def clean_dataframe(df):

    if df.empty:
        return df

    rows_before = len(df)

    # Normalize column names
    df.columns = df.columns.str.lower()

    # Strip strings
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # Convert numeric columns
    numeric_cols = [
        c for c in df.columns
        if "rating" in c or "score" in c or "year" in c
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove duplicates
    df = df.drop_duplicates()

    # Add ingestion date (important for Data Engineering)
    df["ingestion_date"] = datetime.now().date()

    rows_after = len(df)
    logging.info(f"Rows cleaned {rows_before} -> {rows_after}")

    return df



# Process Files

def process_files(files):

    dfs = []

    for file in files:

        logging.info(f"Processing {file}")

        raw = read_json(file)

        if raw is None:
            continue

        df = flatten_json(raw)

        try:
            year_part = [p for p in file.parts if p.startswith("year=")][0]
            year = int(year_part.split("=")[1])
            df["year"] = year
        except Exception:
            logging.warning(f"Could not extract year from path: {file}")
            df["year"] = 0

        df = clean_dataframe(df)

        if not df.empty:
            dfs.append(df)

    return dfs



# Save Parquet with Partition

def save_parquet(df):

    if df.empty:
        logging.warning("Empty dataframe, nothing to save")
        return

    PROCESSED_PATH.mkdir(parents=True, exist_ok=True)

    # Ensure required columns exist
    if "year" not in df.columns:
        logging.warning("No 'year' column found, using fallback")
        df["year"] = 0

    if "ingestion_date" not in df.columns:
        df["ingestion_date"] = datetime.now().date()

    partition_cols = ["year", "ingestion_date"]

    output_path = PROCESSED_PATH / "nhtsa_data"

    df.to_parquet(
        output_path,
        engine="pyarrow",
        partition_cols=partition_cols,
        index=False
    )

    logging.info(f"Saved parquet dataset to {output_path}")
    logging.info(f"Partitions: {partition_cols}")
    logging.info(f"Total rows written: {len(df)}")



# Main Transform Function

def transform_nhtsa():

    files = get_raw_files()

    if not files:
        logging.warning("No raw files found")
        return

    dfs = process_files(files)

    if not dfs:
        logging.warning("No valid dataframes produced")
        return

    final_df = pd.concat(dfs, ignore_index=True)

    save_parquet(final_df)



# Entry Point

def main():

    logging.info("Starting NHTSA transformation")

    try:
        transform_nhtsa()
        logging.info("Transformation finished successfully")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()