CREATE TABLE IF NOT EXISTS stg_nhtsa (
    vehicle_make TEXT,
    vehicle_model TEXT,
    recall_id TEXT,
    year INT,
    ingestion_date DATE
);

CREATE INDEX IF NOT EXISTS idx_stg_nhtsa_vehicle
ON stg_nhtsa (vehicle_make, vehicle_model);

CREATE INDEX IF NOT EXISTS idx_stg_nhtsa_recall
ON stg_nhtsa (recall_id);