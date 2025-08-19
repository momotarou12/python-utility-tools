import pandas as pd
import sys
import os
import re
import pyarrow.parquet as pq

# --- Command-line argument processing ---
# Check if the script is executed with the file path and limit size (e.g., 1GiB, 500MB) as arguments.
# If the number of arguments is not exactly 3, display usage and exit.
if len(sys.argv) != 3:
    print("Usage: python check_parquet_size.py <path_to_parquet_file> <free_tier_limit>")
    print("Example: python check_parquet_size.py ./data/my_data.parquet 1GiB")
    print("Example: python check_parquet_size.py /path/to/your/file.parquet 500MB")
    sys.exit(1)  # Exit with an error code

# --- Parse the limit size argument and convert to MB ---
# Example: "1GiB" -> 1024, "500MB" -> 500
limit_string = sys.argv[2]
limit_mb = None

# Regular expression to extract the numerical value and unit (GiB or MB)
match = re.match(r'^(\d+(\.\d+)?)\s*([gG][iI][bB]|[mM][bB])$', limit_string)

if match:
    value = float(match.group(1))
    unit = match.group(3).upper()  # Convert unit to uppercase

    if unit == 'GIB':
        limit_mb = value * 1024
    elif unit == 'MB':
        limit_mb = value
else:
    print(f"Error: Invalid free tier limit format: {limit_string}. Please use a format like '1GiB' or '500MB'.")
    sys.exit(1)
    
# Get the first command-line argument as the Parquet file path.
# Use the absolute path for file checking.
parquet_file_path = sys.argv[1]

# Check if the file actually exists.
# If it doesn't exist, display an error message and exit the script.
if not os.path.exists(parquet_file_path):
    print(f"Error: File not found at {parquet_file_path}")
    sys.exit(1)

# --- Main processing ---
try:
    # --- Data loading and memory usage estimation using Pandas ---
    # Load the entire file (if memory allows).
    large_file_df = pd.read_parquet(parquet_file_path)
    print("--- Pandas DataFrame Information ---")
    print(f"DataFrame info (full file):")
    large_file_df.info(memory_usage='deep')
    print(f"\nApproximate total memory usage (full file): {large_file_df.memory_usage(deep=True).sum() / (1024**2):.2f} MB")

    # Get the memory usage calculated by Pandas' memory_usage('deep').
    estimated_memory_usage_mb = large_file_df.memory_usage(deep=True).sum() / (1024**2)

    # Pandas' memory_usage('deep') estimates the actual size that the DataFrame and its contents
    # (especially objects like strings) occupy in memory. This includes Pandas' internal structure and overhead.

    # --- Uncompressed size estimation from metadata using PyArrow ---
    # This section estimates the uncompressed size of the data by getting the uncompressed size
    # of each column from the Parquet file's metadata and summing them up.
    # This value is different from the Pandas memory usage and is closer to the physical size of the data itself.
    parquet_file = pq.ParquetFile(parquet_file_path)
    total_uncompressed_bytes = 0
    # Loop through each row group.
    for i in range(parquet_file.num_row_groups):
        row_group_meta = parquet_file.metadata.row_group(i)
        # Loop through each column and get the uncompressed size to sum them up.
        for j in range(row_group_meta.num_columns):
            total_uncompressed_bytes += row_group_meta.column(j).total_uncompressed_size

    # Convert the total uncompressed size to MB and display.
    # The Estimated Uncompressed Size is the estimated size of the Parquet file's data section if it were uncompressed.
    # This is often smaller than the Pandas memory usage.
    print("\n--- PyArrow Metadata Information ---")
    print(f"Estimated Uncompressed Size (from metadata): {total_uncompressed_bytes / (1024**2):.2f} MB")

    # Also display Parquet file metadata and schema.
    print(f"\nParquet File Metadata:")
    print(parquet_file.metadata)
    print(f"\nParquet File Schema:")
    print(parquet_file.schema)

    # --- Comparison with the specified limit and percentage display ---
    # Compare the estimated in-memory size from Pandas with the input free tier size.
    print(f"\n--- Specified Limit Estimation ---")
    print(f"Using specified limit: {limit_string} ({limit_mb:.2f} MB)")

    if estimated_memory_usage_mb <= limit_mb:
        print(f"✅ Estimated size ({estimated_memory_usage_mb:.2f} MB) fits within the specified limit.")
    else:
        print(f"❌ Estimated size ({estimated_memory_usage_mb:.2f} MB) exceeds the specified limit.")

    # Calculate and display the usage percentage.
    usage_percentage = (estimated_memory_usage_mb / limit_mb) * 100 if limit_mb > 0 else 0
    print(f"This represents {usage_percentage:.2f}% of the specified limit.")

except FileNotFoundError:
    print(f"Error: File not found at {parquet_file_path}")
except Exception as e:
    print(f"An error occurred: {e}")