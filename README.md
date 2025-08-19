# Python Utility Scripts / Parquet Size Checker

A collection of small Python scripts for development and data analysis utilities.

## check_parquet_size.py

**Purpose:**

This script analyzes a Parquet file to provide key information about its structure and size. It estimates the memory usage when loaded into a Pandas DataFrame and the estimated uncompressed data size based on Parquet metadata. It can also check if the estimated size fits within a specified storage limit (e.g., Cloud Firestore free tier) and calculate the percentage of the limit used.

This tool is useful for understanding the scale of your data and assessing storage requirements for databases or in-memory processing.

**Prerequisites:**

*   Python 3
*   pandas library
*   pyarrow library

**Installation:**

You can install the required Python libraries using pip:


**How to Run:**

The script is executed from the command line and requires two arguments: the path to the Parquet file and the storage limit with a unit (GiB or MB).
*   `<path_to_parquet_file>`: The file path to the .parquet file you want to analyze.
*   `<storage_limit>`: The storage limit you want to check against, followed by 'GiB' or 'MB' (e.g., `1GiB`, `500MB`, `2.5GiB`).

**Examples:**

Check if `./data/my_data.parquet` fits within a 1 GiB limit:

Check if `/path/to/your/file.parquet` fits within a 500 MB limit:



**Interpreting the Results:**

The script will output several sections:

1.  **Pandas DataFrame Information:**
    *   Provides standard Pandas `info()` output, showing column names, non-null counts, and data types.
    *   **Approximate total memory usage (full file):** This is the estimated amount of RAM the data would occupy if fully loaded into a Pandas DataFrame using the 'deep' memory usage calculation. This includes Pandas' internal overhead and representation (especially for strings). It's a practical estimate of the in-memory footprint.

2.  **Estimated Uncompressed Size (from metadata):**
    *   This is an estimation of the size of the raw data within the Parquet file if it were not compressed. It's calculated by summing the uncompressed size of each column chunk as reported in the Parquet file's metadata.
    *   This value is often significantly smaller than the Pandas memory usage because it doesn't include Pandas' overhead and reflects the efficiency of the Parquet storage format itself.

3.  **Parquet File Metadata & Schema:**
    *   Displays the low-level metadata (like number of rows, columns, row groups, format version) and the schema (column names and PyArrow data types) directly from the Parquet file. Useful for technical details about the file structure.

4.  **Specified Limit Estimation:**
    *   **Using specified limit:** Shows the limit you provided and its value in MB.
    *   **Estimated size (...) fits within/exceeds the specified limit:** Compares the **Approximate total memory usage (Pandas size)** with the limit you provided and tells you if it fits.
    *   **This represents ...% of the specified limit:** Calculates and displays the percentage of the specified limit that the **Approximate total memory usage (Pandas size)** occupies. This gives you a clear picture of how much of your limit the data consumes.

**Contribution:**

Feel free to fork this repository and contribute improvements!

**License:**

[Mention the license chosen during repository creation, e.g., MIT License]

