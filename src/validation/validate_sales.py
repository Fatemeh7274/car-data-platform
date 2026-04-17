import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

RAW_DIR = Path("external_data/sales")

def validate_sales():

    logger.info("Starting SALES validation")

    files = sorted(RAW_DIR.rglob("*.csv"))

    if not files:
        raise ValueError("No sales raw files found")

    latest_file = files[-1]

    df = pd.read_csv(latest_file)

    if df.empty:
        raise ValueError("Empty sales dataset")

    required_cols = ["Make", "Model"]

    missing = [c for c in required_cols if c not in df.columns]

    if missing:
        raise ValueError(f"Missing columns: {missing}")

    logger.info(f"SALES validation PASSED | rows: {len(df)}")