CREATE TABLE IF NOT EXISTS stg_sales (
    sale_date DATE,
    vehicle_make TEXT,
    vehicle_model TEXT,
    units_sold INT,
    year INT,
    month INT
);

CREATE INDEX IF NOT EXISTS idx_stg_sales_date
ON stg_sales (sale_date);

CREATE INDEX IF NOT EXISTS idx_stg_sales_vehicle
ON stg_sales (vehicle_make, vehicle_model);