#  Car Data Platform (End-to-End Data Engineering Pipeline)

##  Overview
This project is an end-to-end Data Engineering pipeline that collects automotive-related datasets from multiple sources, processes them through a multi-layer ETL architecture, validates data quality, and loads the final structured data into a PostgreSQL Data Warehouse for analytics and reporting.

The pipeline is orchestrated using **Apache Airflow** and follows a modular and scalable architecture inspired by real-world data platforms.

---

##  Architecture

Data Sources (FRED / NHTSA / Sales Data)
↓
Ingestion Layer
↓
Staging Layer
↓
Transformation Layer
↓
Validation & Quality Checks
↓
Data Warehouse (Star Schema)
↓
Analytics / Power BI Dashboard


---

##  Tech Stack

- Python (ETL logic)
- Apache Airflow (Orchestration)
- PostgreSQL (Data Warehouse)
- SQL (Data modeling & queries)
- Power BI (Visualization)
- Docker (Environment setup)
- Pandas (Data processing)

---

## Data Architecture

The project uses a **Star Schema Design**:

### Fact Tables:
- sales_fact
- vehicle_sales_fact
- economic_indicators_fact

### Dimension Tables:
- dim_date
- dim_vehicle
- dim_region
- dim_source

This structure allows efficient analytical querying and BI integration.

---

##  ETL Pipeline Flow

### 1. Ingestion
- Fetch data from multiple sources (APIs / files)
- Store raw data for traceability

### 2. Transformation
- Data cleaning (null handling, formatting, normalization)
- Standardization across sources

### 3. Validation
- Schema validation
- Data quality checks
- Missing / duplicate detection

### 4. Load
- Load transformed data into PostgreSQL staging tables
- Move curated data into Data Warehouse layer

---

## 🧠 Key Features

- Modular ETL design (scalable architecture)
- Multi-source data integration
- Data quality validation layer
- Star schema modeling for analytics
- Airflow DAG orchestration
- Separation of staging, transformation, and warehouse layers

---

##  Project Structure
src/
    ingestion/
    transform/
    load/
    validation/
    quality/
    sql/
dags/


---

##  How to Run

```bash
# 1. Clone repository
git clone https://github.com/Fatemeh7274/car-data-platform.git

# 2. Start Docker environment
docker compose up -d

# 3. Access Airflow UI
http://localhost:8080

-----------------------------------------

Future Improvements
Add CI/CD pipeline (GitHub Actions)
Implement real-time streaming (Kafka)
Add data observability tools
Expand data warehouse with more dimensions
Add automated data testing framework


--Author
Fatemeh Sarikhani
Data Engineering Project – Portfolio Repository


