import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

STAGING_PATH = Path("data/staging/nhtsa/nhtsa_data")

def check_nhtsa_quality():

    logger.info("Starting NHTSA quality check")

    df = pd.read_parquet(STAGING_PATH)

    if df.empty:
        raise ValueError("Empty NHTSA dataset")

    if "year" not in df.columns:
        raise ValueError("Missing year column")

    if df.duplicated().sum() > 0:
        raise ValueError("Duplicate rows detected")

    logger.info(f"NHTSA quality PASSED | rows: {len(df)}")