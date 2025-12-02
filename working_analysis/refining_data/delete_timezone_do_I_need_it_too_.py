import pandas as pd

# Load the CSV data into a DataFrame
file_path = r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/Austin/new_threshold_analysis/graphs/csv_output/combined_output.csv"
df = pd.read_csv(file_path)

# Function to extract timezone and remove it from the datetime string
def extract_timezone(datetime_str):
    if pd.isna(datetime_str):
        return pd.NA, datetime_str
    tz = datetime_str[-5:]
    dt = datetime_str[:-5]
    return tz, dt

# Apply the function to extract timezone information and remove it from the datetime strings
df['start_datetime_timezone'], df['start_datetime'] = zip(*df['start_datetime'].apply(extract_timezone))
df['end_datetime_timezone'], df['end_datetime'] = zip(*df['end_datetime'].apply(extract_timezone))

# Function to ensure proper datetime format
def format_datetime(datetime_str):
    if len(datetime_str) == 19:
        return datetime_str
    return datetime_str[:19]

# Apply the function to ensure proper datetime format
df['start_datetime'] = df['start_datetime'].apply(format_datetime)
df['end_datetime'] = df['end_datetime'].apply(format_datetime)

# Save the modified DataFrame to a new CSV file
output_file_path = r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/Austin/new_threshold_analysis/graphs/modified_combined_V1.csv"
df.to_csv(output_file_path, index=False)

print(df)
