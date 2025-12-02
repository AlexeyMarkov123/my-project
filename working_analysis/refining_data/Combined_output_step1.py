import pandas as pd
import glob
import os

# Define a function to read and concatenate all CSV files
def combine_csv_files(file_path_pattern):
    # Use glob to find all files matching the pattern
    all_files = glob.glob(file_path_pattern)
    
    # Create an empty list to hold the dataframes
    df_list = []
    
    # Loop through the files and read them into a dataframe
    for file in all_files:
        df = pd.read_csv(file)
        df_list.append(df)
    
    # Concatenate all dataframes in the list
    combined_df = pd.concat(df_list, ignore_index=True)
    
    return combined_df

# Define the path pattern to your CSV files
file_path_pattern = r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/LA/temp_analysis_sensor/csv_output/*.csv"

# Combine the CSV files
combined_df = combine_csv_files(file_path_pattern)

# Define the output path
output_path = r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/LA/temp_analysis_sensor/csv_output/combined_output.csv"

# Save the combined dataframe to a CSV file
combined_df.to_csv(output_path, index=False)

print(f"Combined CSV saved to: {output_path}")
