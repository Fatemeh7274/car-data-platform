## High-Level Architecture Diagram

         +------------------+
         |   Data Sources   |
         | (APIs / CSV)     |
         +--------+---------+
                  |
                  v
         +------------------+
         |  Ingestion Layer |
         | (Batch Jobs)     |
         +--------+---------+
                  |
                  v
         +------------------+
         |   Raw Data Lake  |
         | (Immutable Files)|
         +--------+---------+
                  |
                  v
         +------------------+
         | Transformation   |
         +--------+---------+
                  |
                  v
         +------------------+
         | Analytics Layer  |
         +------------------+



 Overview:
This project implements a multi-source batch data platform designed to ingest, store, and prepare automotive and macroeconomic datasets for analytics.

The platform integrates:
-Manufacturer data from the National Highway Traffic Safety Administration (NHTSA)
-Macroeconomic indicators from FRED
-External CSV datasets

Architectural Principles
Architectural Principles The system was designed based on the following principles:
-Immutability: Raw data is never modified after ingestion. 
-Reproducibility: The entire environment can be recreated using requirements.txt.
-Observability: Each ingestion job produces metadata for monitoring and auditing. Scalability (Batch-Oriented): Designed to support additional sources with minimal refactoring.

Data Sources:
1-NHTSA API:
Source: National Highway Traffic Safety Administration 
Type: REST API Data 
Format: JSON U
update Frequency: Periodic
Used for: Manufacturer reference data

FRED API Source: 
Federal Reserve Economic Data 
Type: REST API Data 
Format: JSON (Time Series) 
Update Frequency: Monthly / Quarterly 
Used for: Unemployment rate CPI Interest rate GDP Consumer sentiment

Ingestion Strategy 
Processing Type: Batch Batch 
ingestion was selected due to: 
Moderate data volume 
No real-time business requirement 
API rate limitations 
Streaming architecture was intentionally not selected to keep the system maintainable and cost-efficient. 
Data Loading Strategy Source Strategy Reason NHTSA Append Historical snapshot storage FRED Incremental Time-series updates

Storage Design
Raw Data Layer:
All data is stored in a file-based data lake structure:
data/raw/
    nhtsa/
    fred/
Partitioning strategy:
year=YYYY/month=MM/day=DD
Benefits:
Efficient reprocessing
Easy partition-based querying
Auditability
Metadata Layer:Each ingestion job generates metadata stored under:
data/metadata/

Tracked fields include:
Ingestion timestamp
Record count
Status (SUCCESS / FAILED / NO_NEW_DATA)
Date range (for time-series data)
This enables:
Monitoring
Failure tracking
Pipeline observability


Orchestration (Future Enhancement)
The pipeline is currently script-based but designed to be orchestrated using:
Apache Airflow (planned)
This would enable:
Dependency management
Scheduled batch execution
Monitoring dashboards