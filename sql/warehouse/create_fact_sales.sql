CREATE TABLE IF NOT EXISTS fact_sales (
    id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(date_id),
    vehicle_id INT REFERENCES dim_vehicle(vehicle_id),
    units_sold INT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fact_sales_unique
ON fact_sales (date_id, vehicle_id);