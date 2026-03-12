import json
import logging
from pathlib import Path
import pandas as pd

RAW_PATH = Path("data/raw/fred")
PROCESSED_PATH = Path("data/processed/fred")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


EXPECTED_SCHEMA = {
    "observation_date": "datetime64[ns]",
    "value": "float64",
    "series_id": "object"
}


def validate_schema(df):

    for column in EXPECTED_SCHEMA:
        if column not in df.columns:
            raise ValueError(f"Missing column: {column}")


def clean_dataframe(df, series_id):

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df = df.dropna(subset=["value", "date"]).copy()

    df["series_id"] = series_id

    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    df = df.rename(columns={"date": "observation_date"})

    return df


def load_raw_files(series_id):

    series_path = RAW_PATH / f"series_id={series_id}"

    files = list(series_path.glob("*.json"))

    if not files:
        logging.warning(f"No raw files found for {series_id}")
        return pd.DataFrame()

    dfs = []

    for file in files:

        logging.info(f"Processing {file}")

        with open(file) as f:
            raw = json.load(f)

        observations = raw.get("observations", [])

        df = pd.DataFrame(observations)

        if df.empty:
            logging.warning(f"No observations in {file}")
            continue

        df = clean_dataframe(df, series_id)

        dfs.append(df)
        if not dfs:
         return pd.DataFrame()

    final_df = pd.concat(dfs, ignore_index=True)

    validate_schema(final_df)

    return final_df


def save_parquet(df, series_id):

    output_dir = PROCESSED_PATH / f"series_id={series_id}"

    output_dir.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        output_dir,
        engine="pyarrow",
        partition_cols=["year", "month"],
        index=False
    )

    logging.info(f"Saved parquet for {series_id}")


def transform_series(series_id):

    df = load_raw_files(series_id)

    if df.empty:
        logging.warning(f"No transformed data for {series_id}")
        return

    save_parquet(df, series_id)


def main():

    series_list = [
        "UNRATE",
        "CPIAUCSL",
        "FEDFUNDS",
        "GDP",
        "UMCSENT"
    ]

    for series_id in series_list:
        try:
            logging.info(f"Starting transform for {series_id}")
            transform_series(series_id)

        except Exception as e:
            logging.error(f"Transform failed for {series_id}: {e}")


if __name__ == "__main__":
    main()