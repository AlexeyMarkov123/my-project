import pandas as pd

# Load the CSV file
file_path = r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/Austin/new_threshold_analysis/graphs/csv_output/modified_combined_V1.csv"
df = pd.read_csv(file_path)

# Extract the timezone information
df['start_datetime_timezone'] = df['start_datetime'].str.extract(r'([-+]\d{2}:\d{2})')
df['end_datetime_timezone'] = df['end_datetime'].str.extract(r'([-+]\d{2}:\d{2})')

# Remove the timezone from the original datetime columns
df['start_datetime'] = df['start_datetime'].str.replace(r'([-+]\d{2}:\d{2})', '', regex=True)
df['end_datetime'] = df['end_datetime'].str.replace(r'([-+]\d{2}:\d{2})', '', regex=True)

# Save the modified DataFrame to a new CSV file
new_file_path = r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/Austin/new_threshold_analysis/graphs/csv_outputrial.csv"
df.to_csv(new_file_path, index=False)

# Display the modified DataFrame to the user
import ace_tools as tools; tools.display_dataframe_to_user(name="Modified Data", dataframe=df)
