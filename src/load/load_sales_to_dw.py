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

logger = logging.getLogger("sales_dw_loader")



# MAIN FUNCTION

def load_sales_to_dw():

    logger.info("Starting SALES DW load")

    engine = create_engine(DB_URI)

    with engine.begin() as conn:


        # 1. DIM DATE
        logger.info("Loading dim_date")

        conn.execute(text("""
            INSERT INTO dim_date (full_date, year, month, month_name)
            SELECT DISTINCT
                sale_date,
                year,
                month,
                TO_CHAR(sale_date, 'Month')
            FROM stg_sales
            ON CONFLICT (full_date) DO NOTHING
        """))


        # 2. DIM VEHICLE

        logger.info("Loading dim_vehicle")

        conn.execute(text("""
            INSERT INTO dim_vehicle (make, model)
            SELECT DISTINCT
                vehicle_make,
                vehicle_model
            FROM stg_sales
            ON CONFLICT (make, model) DO NOTHING
        """))


        # 3. FACT SALES
        logger.info("Loading fact_sales")

        conn.execute(text("""
            INSERT INTO fact_sales (date_id, vehicle_id, units_sold)
            SELECT
                d.date_id,
                v.vehicle_id,
                s.units_sold
            FROM stg_sales s
            JOIN dim_date d
                ON s.sale_date = d.full_date
            JOIN dim_vehicle v
                ON s.vehicle_make = v.make
                AND s.vehicle_model = v.model
            ON CONFLICT (date_id, vehicle_id) DO NOTHING
        """))

    logger.info("SALES DW load completed")



# ENTRY

if __name__ == "__main__":
    load_sales_to_dw()