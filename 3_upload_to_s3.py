# 3_upload_to_s3.py
# Author: Yash Jadhav
# Phase 3: Modified to upload all processed historical data to S3.

import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# --- Configuration ---
S3_BUCKET_NAME = "sec-financial-data-yash-07082025" 
PROCESSED_DATA_PATH = "processed_data"

# --- Main Logic ---
def upload_historical_data_to_s3():
    """
    Uploads all processed Parquet files to the S3 gold layer.
    """
    print("--- Starting Phase 3: Uploading historical data to S3 ---")
    s3_client = boto3.client('s3')

    try:
        # Find all .parquet files in the processed_data directory
        files_to_upload = [f for f in os.listdir(PROCESSED_DATA_PATH) if f.endswith('.parquet')]
        
        if not files_to_upload:
            print("No processed files found to upload.")
            return

        print(f"Found {len(files_to_upload)} files to upload to the Gold layer (s3://{S3_BUCKET_NAME}/gold/).")
        
        for filename in files_to_upload:
            local_path = os.path.join(PROCESSED_DATA_PATH, filename)
            s3_key = f"gold/{filename}"
            print(f"Uploading {filename}...")
            s3_client.upload_file(local_path, S3_BUCKET_NAME, s3_key)
            
        print("All processed files uploaded successfully.")

    except (NoCredentialsError, ClientError) as e:
        print(f"Error: AWS credentials not found or invalid. Please run 'aws configure'.")
        return
    except FileNotFoundError:
        print(f"Error: The directory '{PROCESSED_DATA_PATH}' was not found. Please run Phase 2 first.")
        
    print("\n--- Phase 3 Complete ---")

if __name__ == "__main__":
    upload_historical_data_to_s3()
