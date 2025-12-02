import pandas as pd
from datetime import datetime, timedelta

# Define the exclusion periods
exclusion_periods = {
    'Monday': ('01:00', '04:00'),
    'Tuesday': ('01:00', '04:00'),
    'Wednesday': ('01:00', '04:00'),
    'Thursday': ('01:00', '04:00'),
    'Friday': ('01:00', '04:00'),
    'Saturday': ('03:00', '06:00'),
    'Sunday': ('03:00', '06:00')
}

def apply_exclusion_correct(row):
    start = datetime.strptime(row['start_datetime'], '%Y-%m-%dT%H:%M:%S')
    end = datetime.strptime(row['end_datetime'], '%Y-%m-%dT%H:%M:%S')

    if start >= end:
        return []

    new_rows = []
    current_start = start

    while current_start < end:
        day_name = current_start.strftime('%A')
        exclusion_start_time = exclusion_periods[day_name][0]
        exclusion_end_time = exclusion_periods[day_name][1]
        
        exclusion_start = datetime.strptime(current_start.strftime('%Y-%m-%d') + 'T' + exclusion_start_time + ':00', '%Y-%m-%dT%H:%M:%S')
        exclusion_end = datetime.strptime(current_start.strftime('%Y-%m-%d') + 'T' + exclusion_end_time + ':00', '%Y-%m-%dT%H:%M:%S')

        next_day = current_start + timedelta(days=1)
        if current_start >= exclusion_end:
            exclusion_start = datetime.strptime(next_day.strftime('%Y-%m-%d') + 'T' + exclusion_start_time + ':00', '%Y-%m-%dT%H:%M:%S')
            exclusion_end = datetime.strptime(next_day.strftime('%Y-%m-%d') + 'T' + exclusion_end_time + ':00', '%Y-%m-%dT%H:%M:%S')

        if current_start < exclusion_start and end <= exclusion_start:
            if current_start < end:
                new_rows.append([row['type'], row['location_name'], row['area_name'], row['event_name'], current_start.strftime('%Y-%m-%dT%H:%M:%S'), end.strftime('%Y-%m-%dT%H:%M:%S'), row['start_datetime_timezone'], row['end_datetime_timezone']])
            break

        if current_start < exclusion_start and end > exclusion_start:
            if current_start < exclusion_start:
                new_rows.append([row['type'], row['location_name'], row['area_name'], row['event_name'], current_start.strftime('%Y-%m-%dT%H:%M:%S'), exclusion_start.strftime('%Y-%m-%dT%H:%M:%S'), row['start_datetime_timezone'], row['end_datetime_timezone']])
            current_start = exclusion_end

        if current_start >= exclusion_end and end <= exclusion_end + timedelta(days=1):
            if current_start < end:
                new_rows.append([row['type'], row['location_name'], row['area_name'], row['event_name'], current_start.strftime('%Y-%m-%dT%H:%M:%S'), end.strftime('%Y-%m-%dT%H:%M:%S'), row['start_datetime_timezone'], row['end_datetime_timezone']])
            break

        if current_start < exclusion_end and end > exclusion_end:
            current_start = exclusion_end
        elif current_start < exclusion_end and end <= exclusion_end:
            current_start = exclusion_end

    return new_rows

# Load the data
file_path = r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/LA/graph/csv_output/modified_combined_V1.csv" # Replace with your file path
df = pd.read_csv(file_path)

# Apply the refined function to the dataframe
new_rows_corrected = []
for index, row in df.iterrows():
    new_rows_corrected.extend(apply_exclusion_correct(row))

# Create a new dataframe with the modified data
columns_corrected = ['type', 'location_name', 'area_name', 'event_name', 'start_datetime', 'end_datetime', 'start_datetime_timezone', 'end_datetime_timezone']
df_modified_corrected = pd.DataFrame(new_rows_corrected, columns=columns_corrected)

# Save the modified dataframe
modified_file_path_corrected = r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/LA/graph/csv_output/servicetime.csv"  # Replace with your desired output file path
df_modified_corrected.to_csv(modified_file_path_corrected, index=False)

print(f"Processed data saved to {modified_file_path_corrected}")
