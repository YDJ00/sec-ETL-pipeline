# 2_process_data.py
# Author: Yash Jadhav
# Phase 2: Final version using chunking to process very large files with limited memory.

import pandas as pd
import os
import gc # Import the garbage collection module

# --- Configuration ---
YEARS = [2023, 2024]
QUARTERS = [1, 2, 3, 4]
PROCESSED_DATA_PATH = "processed_data"
CHUNK_SIZE = 500000 # Process 5 lakh rows at a time

# --- Main Logic ---
def process_historical_data():
    """
    Reads raw SEC data files in chunks to handle very large datasets with limited memory.
    """
    print("--- Starting Phase 2 (Chunking Enabled): Historical Data Processing ---")

    if not os.path.exists(PROCESSED_DATA_PATH):
        os.makedirs(PROCESSED_DATA_PATH)

    for year in YEARS:
        for quarter in QUARTERS:
            if year == 2024 and quarter > 1:
                continue

            print(f"\n----- Processing: {year} Q{quarter} -----")
            raw_data_path = os.path.join("data", f"{year}q{quarter}")
            output_filename = os.path.join(PROCESSED_DATA_PATH, f"{year}_Q{quarter}_processed.parquet")

            if os.path.exists(output_filename):
                print(f"Result file already exists. Skipping.")
                continue

            sub_file = os.path.join(raw_data_path, "sub.txt")
            num_file = os.path.join(raw_data_path, "num.txt")
            tag_file = os.path.join(raw_data_path, "tag.txt")

            try:
                print("Step 1: Loading static data files...")
                df_sub = pd.read_csv(sub_file, sep='\t', dtype={'cik': 'string', 'adsh': 'string'})
                df_tag = pd.read_csv(tag_file, sep='\t', dtype='string')
                
                # Convert to category for memory efficiency
                df_sub['name'] = df_sub['name'].astype('category')
                df_tag['tag'] = df_tag['tag'].astype('category')

                # --- CHUNKING LOGIC ---
                print(f"Step 2: Processing large 'num.txt' file in chunks of {CHUNK_SIZE} rows...")
                
                processed_chunks = []
                num_dtypes = {
                    'adsh': 'string', 'tag': 'category', 'version': 'string',
                    'coreg': 'string', 'ddate': 'int32', 'qtrs': 'int8',
                    'uom': 'string', 'value': 'float64', 'footnote': 'string'
                }

                # Create an iterator to read the num.txt file in chunks
                num_reader = pd.read_csv(num_file, sep='\t', dtype=num_dtypes, chunksize=CHUNK_SIZE)
                
                for i, num_chunk in enumerate(num_reader):
                    print(f"  - Processing chunk {i+1}...")
                    
                    # Filter and select columns for the current chunk
                    num_chunk = num_chunk[num_chunk['uom'] == 'USD']
                    num_chunk = num_chunk[['adsh', 'tag', 'ddate', 'qtrs', 'value']]
                    
                    # Merge the current chunk with the static data
                    merged_chunk = pd.merge(num_chunk, df_sub, on='adsh', how='left')
                    final_chunk = pd.merge(merged_chunk, df_tag, on='tag', how='left')
                    
                    final_chunk.dropna(subset=['name', 'tlabel'], inplace=True)
                    processed_chunks.append(final_chunk)

                print("\nStep 3: Concatenating all processed chunks...")
                if not processed_chunks:
                    print(f"\nWarning: No data to save for {year} Q{quarter} after filtering.")
                    continue

                final_df = pd.concat(processed_chunks, ignore_index=True)
                print(f"  - Final DataFrame has {final_df.shape[0]} rows.")
                
                print(f"\nStep 4: Saving final data to {output_filename}")
                final_df.to_parquet(output_filename, index=False)
                print(f"Successfully processed and saved data for {year} Q{quarter}.")

                # --- MEMORY CLEANUP ---
                del df_sub, df_tag, processed_chunks, final_df
                gc.collect()
                print("  - Memory cleaned up for next iteration.")

            except FileNotFoundError:
                print(f"\nError: Raw data files not found for {year} Q{quarter}. Please run Phase 1. Skipping.")
            except Exception as e:
                print(f"\nAn unexpected error occurred for {year} Q{quarter}: {e}")

    print("\n--- Phase 2 Complete: All historical data processed. ---")

if __name__ == "__main__":
    process_historical_data()
