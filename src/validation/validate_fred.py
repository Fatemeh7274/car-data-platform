import pandas as pd
from pathlib import Path
import logging
import json

logger = logging.getLogger(__name__)

RAW_PATH = Path("data/raw/fred")

def validate_fred():

    logger.info("Starting FRED validation")

    files = list(RAW_PATH.rglob("*.json"))

    if not files:
        raise ValueError("No FRED raw files found")

    valid_count = 0

    for file in files:

        with open(file) as f:
            raw = json.load(f)

        observations = raw.get("observations", [])

        if not observations:
            continue

        df = pd.DataFrame(observations)

        if df.empty:
            continue

        # required columns in raw
        required_cols = ["date", "value"]

        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing columns in FRED raw: {missing}")

        valid_count += len(df)

    if valid_count == 0:
        raise ValueError("No valid FRED data found")

    logger.info(f"FRED validation PASSED | rows: {valid_count}")