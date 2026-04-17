import pandas as pd
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)

RAW_PATH = Path("data/raw/api/nhtsa")

def validate_nhtsa():

    logger.info("Starting NHTSA validation")

    files = list(RAW_PATH.rglob("*.json"))

    if not files:
        raise ValueError("No NHTSA raw files found")

    total_rows = 0

    for file in files:

        with open(file) as f:
            raw = json.load(f)

        if isinstance(raw, dict):
            data = raw.get("results", [])
        else:
            data = raw

        df = pd.json_normalize(data)

        if df.empty:
            continue

        total_rows += len(df)

    if total_rows == 0:
        raise ValueError("No valid NHTSA data found")

    logger.info(f"NHTSA validation PASSED | rows: {total_rows}")