import pandas as pd
from pathlib import Path
import json
import logging

RAW_PATH = Path("data/raw/nhtsa")
PROCESSED_PATH = Path("data/processed/nhtsa")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

#GET RAW FILES

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

#Read raw data

def read_json(file):

    try:
        with open(file) as f:
            raw = json.load(f)

        return raw

    except Exception as e:
        logging.error(f"Failed reading {file}: {e}")
        return None

#Flatten JSON

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
    

# Clean Row Data

def clean_dataframe(df):

    if df.empty:
        return df
    rows_before = len(df)
    df.columns = df.columns.str.lower()

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    numeric_cols = [
        c for c in df.columns
        if "rating" in c or "score" in c or "year" in c
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates()

    if "modelyear" in df.columns:
        df["year"] = df["modelyear"]

    rows_after = len(df)

    logging.info(f"Rows cleaned {rows_before} -> {rows_after}")

    return df

# Make process File

def process_files(files):

    dfs = []

    for file in files:

        logging.info(f"Processing {file}")

        raw = read_json(file)

        if raw is None:
            continue

        df = flatten_json(raw)

        df = clean_dataframe(df)

        if not df.empty:
            dfs.append(df)

    return dfs

# Save parquet

def save_parquet(df):

    PROCESSED_PATH.mkdir(parents=True, exist_ok=True)

    output_file = PROCESSED_PATH / "nhtsa_data.parquet"

    df.to_parquet(
        output_file,
        engine="pyarrow",
        partition_cols=["year"] if "year" in df.columns else None,
        index=False
    )

    logging.info(f"Saved parquet file to {output_file}")
    logging.info(f"Total rows written: {len(df)}")

# Transform Data

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


def main():

    logging.info("Starting NHTSA transformation")

    try:
        transform_nhtsa()
        logging.info("Transformation finished successfully")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()