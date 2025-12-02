import pyarrow.parquet as pq

def print_parquet_header(file_path):
    # Open the Parquet file
    parquet_file = pq.ParquetFile(file_path)

    # Print basic metadata
    print("=== File Metadata ===")
    print(parquet_file.metadata)

    # Print number of row groups
    print("\n=== Row Groups ===")
    print(f"Number of row groups: {parquet_file.num_row_groups}")

    # Print schema
    print("\n=== Schema ===")
    print(parquet_file.schema)

    # Print detailed row group metadata
    for i in range(parquet_file.num_row_groups):
        print(f"\n--- Row Group {i} ---")
        row_group = parquet_file.metadata.row_group(i)
        print(f"Number of rows: {row_group.num_rows}")
        print(f"Total byte size: {row_group.total_byte_size}")
        for j in range(row_group.num_columns):
            col = row_group.column(j)
            print(f"\nColumn {j}: {col.path}")
            print(f" - Type: {col.physical_type}")
            print(f" - Logical Type: {col.logical_type}")
            print(f" - Encodings: {col.encodings}")
            print(f" - Compression: {col.compression}")
            print(f" - Num Values: {col.num_values}")
            print(f" - Total Uncompressed Size: {col.total_uncompressed_size}")
            print(f" - Total Compressed Size: {col.total_compressed_size}")

# Example usage
file_path = "weather_Los_Angeles_2019-1-13_2019-1-20_filtered_v2_clipped_by_Aleksei.parquet"
print_parquet_header(file_path)
