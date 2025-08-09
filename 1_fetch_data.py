# 1_fetch_data.py
# Author: Yash Jadhav
# Phase 1: Modified to fetch historical data for multiple quarters.

import requests
import zipfile
import os
from io import BytesIO

# --- Configuration ---
# Define the range of years and quarters to process.
# This will fetch data from Q1 2023 to Q1 2024.
YEARS = [2023, 2024]
QUARTERS = [1, 2, 3, 4]

# --- Main Logic ---
def fetch_historical_data():
    """
    Downloads and extracts SEC financial data for a specified range of years and quarters.
    """
    print("--- Starting Phase 1: Historical Data Acquisition ---")

    for year in YEARS:
        for quarter in QUARTERS:
            # Skip future quarters (e.g., Q3, Q4 of the current year if they haven't happened yet)
            if year == 2024 and quarter > 1:
                continue

            print(f"\nProcessing: {year} Q{quarter}")
            
            # --- Constants for the current loop ---
            sec_data_url = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{quarter}.zip"
            raw_data_path = os.path.join("data", f"{year}q{quarter}")

            if not os.path.exists(raw_data_path):
                os.makedirs(raw_data_path)
            
            if os.path.exists(os.path.join(raw_data_path, "sub.txt")):
                print(f"Data for {year} Q{quarter} already exists. Skipping download.")
                continue

            try:
                print(f"Downloading data from: {sec_data_url}")
                headers = {'User-Agent': "Yash Jadhav yash918jadhav@gmail.com"}
                response = requests.get(sec_data_url, headers=headers, stream=True)
                response.raise_for_status()

                with zipfile.ZipFile(BytesIO(response.content)) as z:
                    z.extractall(raw_data_path)
                
                print(f"Successfully extracted files for {year} Q{quarter}.")

            except requests.exceptions.HTTPError as e:
                print(f"Warning: Could not download data for {year} Q{quarter}. Status: {e.response.status_code}. It may not be available.")
            except Exception as e:
                print(f"An unexpected error occurred for {year} Q{quarter}: {e}")

    print("\n--- Phase 1 Complete: All historical data downloaded. ---")

if __name__ == "__main__":
    fetch_historical_data()
