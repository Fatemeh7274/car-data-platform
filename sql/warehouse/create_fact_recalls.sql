CREATE TABLE IF NOT EXISTS fact_recalls (
    id SERIAL PRIMARY KEY,
    vehicle_id INT REFERENCES dim_vehicle(vehicle_id),
    recall_id TEXT,
    year INT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fact_recalls_unique
ON fact_recalls (vehicle_id, recall_id);