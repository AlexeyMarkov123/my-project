import pandas as pd

# Define the path to the input CSV file
input_csv_path = r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/LA/graph/csv_output/requested_time_merged_intervals_no_overlapping.csv"
output_csv_path = r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/LA/graph/Merged_NO_Overlapping_Requested_time/merged_intervals_NO_overlapping_result.csv"

# Load the CSV file into a DataFrame
df = pd.read_csv(input_csv_path)

# Convert the 'duration' column to timedelta
df['duration'] = pd.to_timedelta(df['duration'])

# Sum the durations
total_duration = df['duration'].sum()

# Divide the total duration by 3
divided_duration = total_duration / 3

# Calculate the total hours in a year
total_hours_in_year = 7668 #24 * 365 #7668 #for requested time

# Calculate the percentage
percentage = (divided_duration.total_seconds() / 3600) / total_hours_in_year * 100

# Create a result DataFrame
result_df = pd.DataFrame({
    'total_duration': [total_duration],
    'divided_duration': [divided_duration],
    'percentage_of_year': [percentage]
})

# Save the result to a CSV file
result_df.to_csv(output_csv_path, index=False)

# Display the result DataFrame
print(result_df)
