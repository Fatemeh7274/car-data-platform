import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Paths

RAW_DIR = Path("external_data/sales")
STAGING_DIR = Path(r"data\staging\sales")  
STAGING_DIR.mkdir(parents=True, exist_ok=True)
 
# Load latest CSV

def load_latest_raw():
    all_files = sorted(RAW_DIR.rglob("*.csv"))
    if not all_files:
        raise FileNotFoundError(f"No raw sales CSV files found in {RAW_DIR}")
    latest_file = all_files[-1]
    logger.info(f"Loading latest raw CSV: {latest_file}")
    df = pd.read_csv(latest_file)
    return df, latest_file


# Reshape wide → long

def reshape_sales(df):
    logger.info("Reshaping dataset from wide to long format")
    id_columns = ["Make", "Model"]
    value_columns = [c for c in df.columns if c not in id_columns and c != "Logo"]
    df = df.melt(
        id_vars=id_columns,
        value_vars=value_columns,
        var_name="sale_date",
        value_name="units_sold"
    )
    return df


# Standardize columns

def standardize_columns(df):
    logger.info("Standardizing column names")
    df.columns = df.columns.str.strip().str.lower()
    df = df.rename(columns={
        "make": "vehicle_make",
        "model": "vehicle_model"
    })
    return df


# Convert month names

def convert_sale_date(df):
    logger.info("Converting month columns to datetime")
    month_map = {
        "janv": "Jan",
        "mars": "Mar",
        "juin": "Jun",
        "juil": "Jul"
    }
    df["sale_date"] = df["sale_date"].str.replace("-", " ")
    for fr, en in month_map.items():
        df["sale_date"] = df["sale_date"].str.replace(fr, en)
    df["sale_date"] = pd.to_datetime(
        df["sale_date"],
        format="%b %y",
        errors="coerce"
    )
    df["year"] = df["sale_date"].dt.year
    df["month"] = df["sale_date"].dt.month
    return df


# Clean sales data

def clean_sales_data(df):
    logger.info("Cleaning sales data")
    df["units_sold"] = pd.to_numeric(df["units_sold"], errors="coerce")
    df = df.dropna(subset=["sale_date", "vehicle_make", "vehicle_model", "units_sold"])
    return df

# Remove duplicates

def remove_duplicates(df):
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    logger.info(f"Duplicates removed: {before - after}")
    return df


# Save to parquet

def save_parquet(df, input_file):

    date_str = input_file.stem.split("_")[-1]

    output_dir = STAGING_DIR 
    output_dir.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        output_dir,
        engine="pyarrow",
        partition_cols=["year","month"],
        index=False
    )

    logger.info(f"Staging parquet saved with partitions: {output_dir}")



# Main pipeline

def main():
    raw_df, latest_file = load_latest_raw()
    reshaped_df = reshape_sales(raw_df)
    standardized_df = standardize_columns(reshaped_df)
    dated_df = convert_sale_date(standardized_df)
    clean_df = clean_sales_data(dated_df)
    final_df = remove_duplicates(clean_df)
    save_parquet(final_df, latest_file)
    logger.info("Transform Sales completed successfully")

if __name__ == "__main__":
    main()