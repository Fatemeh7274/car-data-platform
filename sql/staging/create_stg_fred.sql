CREATE TABLE IF NOT EXISTS stg_fred (
    observation_date DATE,
    series_id TEXT,
    value FLOAT,
    year INT,
    month INT
);

CREATE INDEX IF NOT EXISTS idx_stg_fred_date
ON stg_fred (observation_date);

CREATE INDEX IF NOT EXISTS idx_stg_fred_series
ON stg_fred (series_id);