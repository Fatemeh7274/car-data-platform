-- STAGING
\i staging/create_stg_sales.sql
\i staging/create_stg_fred.sql
\i staging/create_stg_nhtsa.sql

-- WAREHOUSE
\i warehouse/create_dim_date.sql
\i warehouse/create_dim_vehicle.sql
\i warehouse/create_dim_indicator.sql
\i warehouse/create_fact_sales.sql
\i warehouse/create_fact_macro.sql
\i warehouse/create_fact_recalls.sql