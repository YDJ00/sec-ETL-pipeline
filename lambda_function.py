# lambda_function.py
# Author: Yash Jadhav
# Phase 5: Fully automated ETL pipeline function for AWS Lambda.

import os
import requests
import zipfile
import pandas as pd
import boto3
from datetime import datetime
from io import BytesIO

# --- Environment Variables ---
# These will be set in the Lambda function's configuration.
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
GLUE_CRAWLER_NAME = os.environ.get('GLUE_CRAWLER_NAME')

def get_current_year_quarter():
    """Determines the current year and quarter."""
    now = datetime.utcnow()
    quarter = (now.month - 1) // 3 + 1
    return now.year, quarter

def lambda_handler(event, context):
    """
    The main entry point for the AWS Lambda function.
    This function performs the entire ETL process.
    """
    if not S3_BUCKET_NAME or not GLUE_CRAWLER_NAME:
        error_message = "Error: S3_BUCKET_NAME and GLUE_CRAWLER_NAME environment variables must be set."
        print(error_message)
        return {'statusCode': 400, 'body': error_message}

    # Determine the year and quarter to process.
    # Note: SEC data is often released a few weeks after a quarter ends.
    # For a production system, you might want to process the *previous* quarter.
    # For this example, we'll use the current year and quarter.
    YEAR, QUARTER = get_current_year_quarter()
    
    # Lambda's only writable directory is /tmp. We'll use it for all temporary files.
    TMP_DIR = f"/tmp/{YEAR}q{QUARTER}"
    if not os.path.exists(TMP_DIR):
        os.makedirs(TMP_DIR)

    s3_client = boto3.client('s3')
    glue_client = boto3.client('glue')

    print(f"--- Starting ETL Process for {YEAR} Q{QUARTER} ---")

    # === 1. EXTRACT: Fetch data from SEC ===
    try:
        print("Step 1: Fetching data from SEC EDGAR...")
        sec_url = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{YEAR}q{QUARTER}.zip"
        headers = {'User-Agent': "Yash Jadhav yash918jadhav@gmail.com"}
        response = requests.get(sec_url, headers=headers, stream=True)
        response.raise_for_status()
        
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            z.extractall(TMP_DIR)
        print(f"Successfully extracted files to: {TMP_DIR}")
        
    except requests.exceptions.HTTPError as e:
        error_msg = f"Failed to download data. Status: {e.response.status_code}. The data for {YEAR} Q{QUARTER} might not be available yet."
        print(error_msg)
        return {'statusCode': 404, 'body': error_msg}
    except Exception as e:
        print(f"An error occurred during extraction: {e}")
        return {'statusCode': 500, 'body': str(e)}

    # === 2. TRANSFORM: Process data with Pandas ===
    try:
        print("\nStep 2: Transforming data with Pandas...")
        df_sub = pd.read_csv(os.path.join(TMP_DIR, "sub.txt"), sep='\t', dtype={'cik': str})
        df_num = pd.read_csv(os.path.join(TMP_DIR, "num.txt"), sep='\t')
        df_tag = pd.read_csv(os.path.join(TMP_DIR, "tag.txt"), sep='\t')

        df_num = df_num[df_num['uom'] == 'USD']
        df_sub = df_sub[['adsh', 'cik', 'name', 'form', 'filed']]
        df_num = df_num[['adsh', 'tag', 'ddate', 'qtrs', 'value']]
        df_tag = df_tag[['tag', 'tlabel', 'doc']]

        merged_df = pd.merge(df_num, df_sub, on='adsh', how='left')
        final_df = pd.merge(merged_df, df_tag, on='tag', how='left')
        final_df.dropna(subset=['name', 'tlabel'], inplace=True)
        print(f"Transformation complete. Processed {final_df.shape[0]} rows.")

        processed_parquet_path = os.path.join(TMP_DIR, "processed.parquet")
        final_df.to_parquet(processed_parquet_path, index=False)

    except Exception as e:
        print(f"An error occurred during transformation: {e}")
        return {'statusCode': 500, 'body': str(e)}

    # === 3. LOAD: Upload to S3 and run Crawler ===
    try:
        print("\nStep 3: Uploading to S3 and starting Glue Crawler...")
        # Upload processed data
        s3_key = f"gold/{YEAR}_Q{QUARTER}_processed.parquet"
        s3_client.upload_file(processed_parquet_path, S3_BUCKET_NAME, s3_key)
        print(f"Successfully uploaded processed data to s3://{S3_BUCKET_NAME}/{s3_key}")

        # Start the Glue Crawler
        print(f"Starting Glue crawler: {GLUE_CRAWLER_NAME}")
        glue_client.start_crawler(Name=GLUE_CRAWLER_NAME)
        print("Crawler started successfully.")

    except Exception as e:
        print(f"An error occurred during loading: {e}")
        return {'statusCode': 500, 'body': str(e)}

    print("\n--- ETL Process Complete ---")
    return {
        'statusCode': 200,
        'body': f'Successfully processed and loaded data for {YEAR} Q{QUARTER}.'
    }
