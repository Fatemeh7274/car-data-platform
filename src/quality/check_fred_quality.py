import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

STAGING_PATH = Path("data/staging/fred")

def check_fred_quality():

    logger.info("Starting FRED quality check")

    df = pd.read_parquet(STAGING_PATH)

    if df.empty:
        raise ValueError("Empty FRED dataset after transform")

    if df["value"].isnull().any():
        raise ValueError("Null values found in FRED value")

    if (df["value"] == 0).all():
        raise ValueError("All values are zero (suspicious)")

    logger.info(f"FRED quality PASSED | rows: {len(df)}")