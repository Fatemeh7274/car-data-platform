import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

STAGING_PATH = Path("data/staging/sales")

def check_sales_quality():

    logger.info("Starting SALES quality check")

    df = pd.read_parquet(STAGING_PATH)

    if df.empty:
        raise ValueError("Empty SALES dataset")

    if (df["units_sold"] < 0).any():
        raise ValueError("Negative sales detected")

    if df.duplicated().sum() > 0:
        raise ValueError("Duplicate records detected")

    logger.info(f"SALES quality PASSED | rows: {len(df)}")