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

logger = logging.getLogger("nhtsa_dw_loader")



# MAIN

def load_nhtsa_to_dw():

    logger.info("Starting NHTSA DW load")

    engine = create_engine(DB_URI)

    with engine.begin() as conn:


        # 1. DIM VEHICLE
        logger.info("Updating dim_vehicle")

        conn.execute(text("""
            INSERT INTO dim_vehicle (make, model)
            SELECT DISTINCT
                vehicle_make,
                vehicle_model
            FROM stg_nhtsa
            ON CONFLICT (make, model) DO NOTHING
        """))


        # 2. FACT RECALLS
        logger.info("Loading fact_recalls")

        conn.execute(text("""
            INSERT INTO fact_recalls (vehicle_id, recall_id, year)
            SELECT
                v.vehicle_id,
                s.recall_id,
                s.year
            FROM stg_nhtsa s
            JOIN dim_vehicle v
                ON s.vehicle_make = v.make
                AND s.vehicle_model = v.model
            ON CONFLICT (vehicle_id, recall_id) DO NOTHING
        """))

    logger.info("NHTSA DW load completed")



# ENTRY

if __name__ == "__main__":
    load_nhtsa_to_dw()