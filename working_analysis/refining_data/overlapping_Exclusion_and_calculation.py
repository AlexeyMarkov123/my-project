import pandas as pd

# Load the CSV file into a DataFrame
file_path = r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/LA/graph/csv_output/servicetime.csv"
df = pd.read_csv(file_path)

# Convert start_datetime and end_datetime to datetime format
df['start_datetime'] = pd.to_datetime(df['start_datetime'])
df['end_datetime'] = pd.to_datetime(df['end_datetime'])

# Sort the DataFrame by start_datetime
df = df.sort_values(by='start_datetime')

# Function to merge overlapping intervals
def merge_intervals(df):
    merged_intervals = []
    current_start = df.iloc[0]['start_datetime']
    current_end = df.iloc[0]['end_datetime']
    
    for i in range(1, len(df)):
        row = df.iloc[i]
        if row['start_datetime'] <= current_end:
            # Overlapping interval, merge it
            current_end = max(current_end, row['end_datetime'])
        else:
            # No overlap, add the previous interval to the list
            merged_intervals.append((current_start, current_end))
            # Start a new interval
            current_start = row['start_datetime']
            current_end = row['end_datetime']
    
    # Add the last interval
    merged_intervals.append((current_start, current_end))
    return merged_intervals

# Get the merged intervals
merged_intervals = merge_intervals(df)

# Create a DataFrame for the merged intervals
merged_df = pd.DataFrame(merged_intervals, columns=['start_datetime', 'end_datetime'])

# Calculate the total duration of the merged intervals
merged_df['duration'] = merged_df['end_datetime'] - merged_df['start_datetime']
total_duration = merged_df['duration'].sum()

# Save the merged intervals to a new CSV file
output_file_path = r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/LA/graph/csv_output/requested_time_merged_intervals_no_overlapping.csv"
merged_df.to_csv(output_file_path, index=False)

print(f"The merged intervals CSV file has been saved to: {output_file_path}")
print(f"The total duration without overlapping is: {total_duration}")
