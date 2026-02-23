# 🏀 End-to-End NBA Analytics Data Warehouse

A **fault‑tolerant**, medallion‑architecture data pipeline that ingests NBA team and player statistics from Kaggle, processes them through Bronze → Silver → Gold layers on AWS S3, and serves interactive dashboards via a containerized metadata layer. Orchestration is handled by **Apache Airflow** (Dockerized), data transformations run on **AWS Glue** (Spark SQL), and a **Hive Metastore** (Dockerized) provides schema management for BI tools.

---

## 📌 Overview

This project demonstrates a **hybrid cloud‑native data platform**:
- **Data Lake** on AWS S3 with three quality tiers.
- **Serverless processing** with AWS Glue – no cluster management.
- **Orchestration** via Apache Airflow running in Docker for scheduling, monitoring, and fault tolerance.
- **Metadata management** through a containerized Hive Metastore, enabling SQL engines (Trino, Spark SQL, etc.) to query S3 data directly.
- **Interactive dashboards** built with [Tableau / Apache Superset / Power BI] connected to the metastore.

**Key Features**
- Medallion architecture (Bronze/Silver/Gold) for data quality and governance.
- Fault‑tolerant orchestration with Airflow (retries, alerting, idempotency).
- Containerized control plane – Airflow + Metastore run in Docker for portability.
- Centralized metadata via Hive Metastore (compatible with AWS Glue Data Catalog if needed).
- BI dashboards on Gold‑layer data.

---

## 🏗️ Architecture

![alt text](image-1.png)

Kaggle Dataset
│
▼
┌─────────────────────┐
│ Airflow (Docker) │ (scheduled monthly)
│ - DAGs │
│ - Sensors │
└──────────┬──────────┘
│
├──► S3 (Bronze) - raw data landing zone
│
├──► AWS Glue (Silver) - clean, deduplicate, type cast
│
├──► S3 (Silver) - cleaned, partitioned Parquet
│
├──► AWS Glue (Gold) - build aggregated tables
│
├──► S3 (Gold) - reporting‑ready datasets
│
└──► Hive Metastore (Docker) - table schemas & partitions
│
└──► BI Tool [e.g., Tableau / Superset] - dashboards & ad‑hoc analysis

text

---

## 🛠️ Technologies Used

| Component           | Technology                                    | Purpose                                      |
| ------------------- | --------------------------------------------- | -------------------------------------------- |
| Orchestration       | Apache Airflow (Docker container)             | Schedule and monitor monthly pipeline runs   |
| Data Lake Storage   | Amazon S3                                     | Tiered storage: `bronze/`, `silver/`, `gold/`|
| Data Processing     | AWS Glue (Spark SQL, PySpark)                 | Transform raw data into clean, aggregated tables |
| Metadata Management | Hive Metastore (Docker container)             | Store table schemas, enable SQL queries on S3|
| Query Engine        | [Trino / Spark SQL / Presto]                  | Query Gold/Silver layers via metastore       |
| Visualization       | [Tableau / Apache Superset / Power BI]        | Build interactive dashboards                  |
| Source Data         | Kaggle API                                     | Fetch NBA stats (e.g., `nba-team-stats`)     |
| Container Runtime   | Docker / Docker Compose                        | Run Airflow and Metastore locally or on EC2  |

---

## 📦 Pipeline Details

### 1️⃣ Data Ingestion (Bronze)
- **Airflow DAG** (Docker) triggers monthly (configurable via `schedule_interval`).
- Uses **Kaggle API** to download the latest NBA dataset (CSV/JSON).
- Stores raw files into `s3://your-bucket/bronze/` with partitions by `year` and `month` (e.g., `year=2024/month=02/`).

### 2️⃣ Data Cleansing (Silver)
- **AWS Glue ETL job** (PySpark script) reads from Bronze.
- Performs:
  - Schema enforcement and type casting (e.g., string → integer, date).
  - Handling missing values (drop or impute).
  - Deduplication based on game/player IDs.
- Writes **Parquet** format (optimized for analytics) to `s3://your-bucket/silver/`, partitioned by `season` and `team`.
- **Metastore update:** After the Glue job, an Airflow task runs a Hive/Spark SQL command (via the metastore Thrift endpoint) to register the new partitions.

### 3️⃣ Data Aggregation (Gold)
- **Second Glue job** builds dimensional models:
  - **Player career stats** – averages per season, shooting percentages.
  - **Team performance** – win/loss streaks, offensive/defensive ratings.
  - **Advanced metrics** – PER, usage rate, true shooting percentage.
- Outputs to `s3://your-bucket/gold/` in Parquet, also partitioned for efficient queries.
- **Metastore update:** New Gold tables are registered in the Hive Metastore for BI consumption.

### 4️⃣ Visualization
- BI tool connects to the Hive Metastore via JDBC/Thrift.
- Users can run SQL queries directly on S3 data (metastore provides schema and partition locations).
- Dashboards include:
  - Top scorers over time.
  - Team efficiency comparisons.
  - Interactive filters by player, team, season.
  - Historical trend charts.

---

## ⚙️ Fault Tolerance & Reliability

- **Airflow retries** – failed tasks automatically retry up to 3 times (configurable in DAG).
- **Data validation** – Glue jobs perform row count and schema checks before writing.
- **Idempotent writes** – each run overwrites only the relevant partitions, avoiding duplicates.
- **Monitoring** – Airflow sends alerts (Slack/email) on job failures.
- **S3 versioning** – enabled on Bronze bucket to recover raw data if needed.
- **Container health checks** – Docker Compose ensures Airflow and Metastore services restart on failure.

---

## 🚀 Setup Instructions

### Prerequisites
- AWS account with permissions for S3, Glue, and (optional) Athena.
- Kaggle account and API key.
- Docker and Docker Compose installed (local machine or EC2 instance).
- Python 3.8+ for local development (optional).

### Deployment Steps

#### 1. Clone the repository
```bash
git clone https://github.com/yourusername/nba-data-warehouse.git
cd nba-data-warehouse
2. Configure environment
Copy .env.example to .env and fill in:

init
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_key
S3_BUCKET=your-unique-bucket-name
METASTORE_THRIFT_URI=thrift://metastore:9083   # internal Docker network
Update docker-compose.yml if needed (ports, volumes).

3. Start Airflow and Metastore with Docker
bash
docker-compose up -d
This launches:

Airflow webserver & scheduler (UI at http://localhost:8080)

Hive Metastore (Thrift on port 9083)

(Optional) a query engine like Trino for testing.

4. Set up AWS Glue jobs
Upload the PySpark scripts (scripts/bronze_to_silver.py, scripts/silver_to_gold.py) to an S3 scripts folder (e.g., s3://your-bucket/scripts/).

Create Glue ETL jobs via AWS Console or AWS CLI:

Job 1: Bronze → Silver

Script path: s3://your-bucket/scripts/bronze_to_silver.py

Job parameters: --input_path s3://your-bucket/bronze/, --output_path s3://your-bucket/silver/

Job 2: Silver → Gold

Script path: s3://your-bucket/scripts/silver_to_gold.py

Job parameters: --input_path s3://your-bucket/silver/, --output_path s3://your-bucket/gold/

Ensure Glue has appropriate IAM role with S3 and Glue permissions.

5. Configure Airflow connections
In Airflow UI (http://localhost:8080), add:

AWS connection (Conn Id: aws_default) with your credentials.

Metastore connection (Conn Id: metastore_default, type: Hive, host: metastore, port: 9083) – required if you run post‑processing SQL.

6. Run the DAG
The DAG nba_pipeline is scheduled to run monthly (e.g., at 2 AM on the 1st). You can trigger it manually from the Airflow UI.

Monitor logs in Airflow and AWS Glue console.

7. Connect BI tool to Metastore
Start a Metastore with docker

docker run -d -p 3000:3000 --name metabase metabase/metabase

If running locally: localhost:3000

Use a AWS Athena connector

Point to the Gold tables – the metastore provides schema and partition locations on S3.
```
📊 Example Dashboard
(Insert a screenshot of your final dashboard here)

Player Comparison: Select two players to compare season stats side‑by‑side.

Team Trends: Line chart of points per game over the last 10 seasons.

Leaderboards: Top 10 scorers, rebounders, assist leaders for a given year.

![alt text](image.png)