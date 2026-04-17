CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_id SERIAL PRIMARY KEY,
    make TEXT,
    model TEXT,
    UNIQUE(make, model)
);