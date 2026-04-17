CREATE TABLE IF NOT EXISTS dim_indicator (
    indicator_id SERIAL PRIMARY KEY,
    series_id TEXT UNIQUE
);