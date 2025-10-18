# GlobalMart Retail Data Pipeline

This project builds a robust, end-to-end cloud data pipeline for GlobalMart Retail, leveraging AWS, Snowflake.. It demonstrates modern data engineering practices including diverse ingestion patterns, automated transformations.

## Project Overview

The pipeline collects data from various sources (OLTP database, external API, CSV files), lands it in an AWS S3 data lake, and then processes it through Snowflake's Medallion Architecture (Bronze, Silver, Gold layers) using Streams and Tasks for near real-time transformations. Orchestration can be handled using Github Actions & AWS EventBridge, currently it's just manual running of scripts, while once into S3 buckets, snowflake takes care of itself

##  Key Features

* **Diverse Ingestion Patterns:**
    * **Batch ETL:** Extracts sales data from a PostgreSQL OLTP database, loads it to S3.
    * **API Ingestion:** Fetches product catalog from an external API via AWS Lambda, loads to S3.
    * **Ad-hoc Bulk Load:** Manually uploads customer CSV data to S3, loaded via Snowflake's `COPY INTO` command.
* **Medallion Architecture in Snowflake:**
    * **Bronze Layer:** Raw, immutable data from all sources (Parquet, JSON, CSV).
    * **Silver Layer:** Cleaned, conformed, and structured data with deduplication and type casting, managed by Snowflake Streams and Tasks.
    * **Gold Layer:** Aggregated, business-ready views for analytics and reporting.
* **Automated Transformations:** Snowflake Streams for Change Data Capture (CDC) and Tasks for scheduled `MERGE` operations, ensuring an always up-to-date Silver layer.




* **Orchestration (Future scope of work):** 
    * **GitHub Actions:** Automates the daily batch ETL job and deploys Snowflake SQL changes (CI/CD).
    * **AWS EventBridge:** Schedules the AWS Lambda function for API data ingestion.
* **Secrets Management:** Securely stores database and API credentials using AWS Secrets Manager.
* **CI/CD for Database (DataOps,Future scope of work):** Automates deployment of Snowflake schema changes (tables, views, streams, tasks) directly from Git using GitHub Actions.

## 🛠️ Technologies Used

* **Cloud Provider:** Amazon Web Services (AWS)
    * **Storage:** S3
    * **Serverless Compute:** Lambda
    * **Secrets Management:** Secrets Manager
* **Data Warehouse:** Snowflake
    * **Ingestion:** Snowpipe, Internal Stages, COPY INTO
    * **Transformation:** Streams, Tasks, MERGE, SQL
* **Orchestration & CI/CD:** GitHub Actions
* **Python Libraries:** Pandas, SQLAlchemy, Psycopg2-binary, Boto3, urllib3
* **Version Control:** Git

