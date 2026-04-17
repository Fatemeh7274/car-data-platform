CREATE TABLE IF NOT EXISTS fact_macro (
    id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(date_id),
    indicator_id INT REFERENCES dim_indicator(indicator_id),
    value FLOAT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fact_macro_unique
ON fact_macro (date_id, indicator_id);