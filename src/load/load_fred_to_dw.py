import os
import logging
from sqlalchemy import create_engine, text


# CONFIG

DB_URI = os.getenv(
    "POSTGRES_URI",
    "postgresql://postgres:password@localhost:5432/car_data_platform"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("fred_dw_loader")



# MAIN

def load_fred_to_dw():

    logger.info("Starting FRED → DW load")

    engine = create_engine(DB_URI)

    with engine.begin() as conn:


        # 1. DIM DATE
        logger.info("Loading dim_date")

        conn.execute(text("""
            INSERT INTO dim_date (full_date, year, month, month_name)
            SELECT DISTINCT
                observation_date,
                EXTRACT(YEAR FROM observation_date),
                EXTRACT(MONTH FROM observation_date),
                TO_CHAR(observation_date, 'Month')
            FROM stg_fred
            ON CONFLICT (full_date) DO NOTHING
        """))


        # 2. DIM INDICATOR
        logger.info("Loading dim_indicator")

        conn.execute(text("""
            INSERT INTO dim_indicator (series_id)
            SELECT DISTINCT series_id
            FROM stg_fred
            ON CONFLICT (series_id) DO NOTHING
        """))


        # 3. FACT MACRO
        logger.info("Loading fact_macro")

        conn.execute(text("""
            INSERT INTO fact_macro (date_id, indicator_id, value)
            SELECT
                d.date_id,
                i.indicator_id,
                s.value
            FROM stg_fred s
            JOIN dim_date d
                ON s.observation_date = d.full_date
            JOIN dim_indicator i
                ON s.series_id = i.series_id
            ON CONFLICT (date_id, indicator_id) DO NOTHING
        """))

    logger.info("FRED DW load completed")



# ENTRY

if __name__ == "__main__":
    load_fred_to_dw()