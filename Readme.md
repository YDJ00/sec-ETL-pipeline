# End-to-End ETL Pipeline for SEC Financial Filings

## Overview

This project is a complete, end-to-end data engineering pipeline that automates the process of extracting, transforming, and loading corporate financial data from the U.S. Securities and Exchange Commission (SEC). The system is designed to handle large volumes of historical data, processing quarterly financial statements (10-K, 10-Q filings) and making them available for robust analysis.

The pipeline begins by fetching raw data from the SEC EDGAR system, processes it locally using memory-efficient Python scripts, and then loads it into a modern, multi-layered data lake on AWS S3. By leveraging AWS Glue and Amazon Athena, the processed data is made available through a serverless SQL interface, enabling powerful ad-hoc querying and business intelligence applications. This project showcases a scalable, cost-effective, and production-ready approach to cloud data engineering.

**Author:** Yash Jadhav
**LinkedIn:** www.linkedin.com/in/yash-jadhav-4456952bb
**GitHub:** https://github.com/YDJ00/sec-etl-pipeline

---

## Data Architecture

The pipeline follows a modern lakehouse architecture using a multi-layered approach on AWS:

* **Data Source:** Publicly available quarterly financial statement datasets from the SEC EDGAR system.
* **Local Processing:** Python scripts using the Pandas library for robust, memory-efficient ETL (Extract, Transform, Load) operations, capable of handling large files by processing data in chunks.
* **Data Lake (S3):**
    * **Bronze Layer:** Stores the raw, unmodified `.txt` files downloaded from the SEC for each historical quarter.
    * **Gold Layer:** Stores the cleaned, transformed, and analytics-ready data in the efficient columnar Parquet format.
* **Data Catalog:** AWS Glue is used to crawl the data in the S3 gold layer and create a unified metadata table in the AWS Glue Data Catalog.
* **Query Engine:** Amazon Athena provides a serverless SQL interface to query the entire historical dataset directly from S3, enabling ad-hoc analysis.

---

## Data Scope

This pipeline is configured to download and process the **SEC Financial Statement Datasets** for the last two full calendar years, plus all completed quarters of the current year. This provides a rich historical dataset for time-series analysis and trend identification.

* **Filings:** 10-K (annual) and 10-Q (quarterly) reports.
* **Timeline:** All quarters from 2023 to the most recently completed quarter of 2024.
* **Volume:** Each quarterly file contains several lakh rows of numeric data, resulting in a final dataset of several crore records.

---

## Technologies Used

* **Programming Language:** Python
* **Data Processing:** Pandas
* **Cloud Provider:** Amazon Web Services (AWS)
* **Core AWS Services:**
    * **Amazon S3:** For scalable data lake storage.
    * **AWS Glue:** For data cataloging and schema inference.
    * **Amazon Athena:** For serverless, interactive SQL querying.
* **Version Control:** Git & GitHub

---

## Prerequisites

Before you begin, ensure you have the following installed and configured:

1.  **Python 3.8+:** Make sure Python is installed and accessible from your command line. You can download it from [python.org](https://www.python.org/).
2.  **An AWS Account:** You will need an AWS account with access to S3, Glue, and Athena. A free-tier account is sufficient.
3.  **AWS CLI:** The AWS Command Line Interface is required to configure your local machine to interact with your AWS account.
    * [Installation Guide for AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

---

## Setup and Usage

Follow these steps to set up and run the pipeline.

### 1. Local Setup

First, set up the project on your local machine.

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/YDJ00/sec-etl-pipeline.git](https://github.com/YDJ00/sec-etl-pipeline.git)
    cd sec-etl-pipeline
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    python -m venv venv
    # On Windows
    .\venv\Scripts\activate
    # On macOS/Linux
    source venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install pandas boto3 requests pyarrow
    ```

### 2. AWS Configuration

Next, configure the necessary AWS resources and credentials.

1.  **Create an IAM User for Programmatic Access:**
    * In the AWS Console, go to the **IAM** service.
    * Create a new user (e.g., `sec-pipeline-user`).
    * Attach the following policies: `AmazonS3FullAccess`, `AWSGlueConsoleFullAccess`, and `AmazonAthenaFullAccess`.
    * Create an **access key** and **secret access key** for this user. **Save these credentials securely.**

2.  **Configure the AWS CLI:**
    * Open your terminal and run `aws configure`.
    * Enter the **Access Key ID** and **Secret Access Key** you just created.
    * Set your **Default region name** (e.g., `ap-south-1`).
    * Set the **Default output format** to `json`.

3.  **Create the S3 Bucket:**
    * In the AWS Console, go to the **S3** service.
    * Create a new bucket with a globally unique name (e.g., `sec-financial-data-yourname`).
    * Inside this bucket, create two folders: `gold` and `athena_query_results`.
    * **Note:** You must update the `S3_BUCKET_NAME` variable in the `3_upload_to_s3.py` script to match the name of the bucket you created.

### 3. Pipeline Execution

Now, run the Python scripts in order to execute the ETL process.

1.  **Fetch Raw Data:** This script downloads the last two years of quarterly data.
    ```bash
    python 1_fetch_data.py
    ```

2.  **Process Data Locally:** This script transforms the raw data into Parquet files. It is memory-optimized and may need to be run multiple times if you have limited RAM.
    ```bash
    python 2_process_data.py
    ```

3.  **Upload to S3:** This script uploads all the processed Parquet files to the `gold` folder in your S3 bucket.
    ```bash
    python 3_upload_to_s3.py
    ```

### 4. Data Cataloging and Querying

The final step is to make your data available for analysis.

1.  **Create and Run the Glue Crawler:**
    * In the AWS Glue console, create a new crawler.
    * Point the crawler to your S3 data source: `s3://your-bucket-name/gold/`.
    * Assign an IAM role to the crawler that has permission to read from S3.
    * Create a new database for the output (e.g., `sec_financials_db`).
    * **Run the crawler.** It will scan all the Parquet files and create a unified table named `gold`.

2.  **Query with Athena:**
    * Go to the Amazon Athena query editor.
    * Select the `sec_financials_db` database.
    * You can now run SQL queries on your data!
    ```sql
    SELECT name, tlabel, value, ddate
    FROM "gold"
    WHERE name = 'MICROSOFT CORP'
    ORDER BY ddate DESC;
    ```

### 5. Visualization

Once the data is available in Athena, it is ready to be connected to any major Business Intelligence (BI) tool for visualization and dashboarding.

* **Tableau Public**
* **Microsoft Power BI**
* **Amazon QuickSight**

These tools have built-in connectors for Amazon Athena, allowing you to create rich, interactive dashboards directly from your cloud data lake.
