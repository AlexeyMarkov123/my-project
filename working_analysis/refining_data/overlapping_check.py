import pandas as pd

# Load the CSV file into a DataFrame
file_path = r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/Austin/new_threshold_analysis/graphs/csv_output/modified_combined_V1.csv"
df = pd.read_csv(file_path)

# Convert start_datetime and end_datetime to datetime format
df['start_datetime'] = pd.to_datetime(df['start_datetime'])
df['end_datetime'] = pd.to_datetime(df['end_datetime'])

# Function to check overlap
def check_overlap(row, df):
    overlapping_events = df[(df['start_datetime'] < row['end_datetime']) & (df['end_datetime'] > row['start_datetime'])]
    overlapping_event_names = overlapping_events['event_name'].unique()
    return ', '.join(overlapping_event_names)

# Apply the function to each row
df['overlapping_events'] = df.apply(lambda row: check_overlap(row, df), axis=1)

# Select necessary columns
output_df = df[['event_name', 'start_datetime', 'end_datetime', 'overlapping_events']]

# Save the output to a new CSV file
output_file_path = r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/Austin/new_threshold_analysis/graphs/csv_output/overlapping_check.csv"
output_df.to_csv(output_file_path, index=False)

print(f"The overlapping events CSV file has been saved to: {output_file_path}")
