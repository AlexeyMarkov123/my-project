import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV file into a DataFrame
file_path = r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/Madrid/csv_output/combined_output.csv"
df = pd.read_csv(file_path)

# Specify the event name
#event_name = 'event_wind_gust_speed'
#event_name = 'event_freezing_rain'
#event_name = 'event_hail_amount'
#event_name = 'event_hail_size'
#event_name = 'event_rain'
#event_name = 'event_rain_snow'
#event_name = 'event_snow_rate'
#event_name = 'event_visibility'
#event_name = 'event_snow_rate'
event_name = 'event_temperature'


# Filter the DataFrame based on the specified conditions
filtered_df = df[(df['type'] == 'with_downtime') & (df['event_name'] == event_name)]

# Calculate the duration of each event and add it as a new column
filtered_df['start_datetime'] = pd.to_datetime(filtered_df['start_datetime'])
filtered_df['end_datetime'] = pd.to_datetime(filtered_df['end_datetime'])
filtered_df['duration'] = (filtered_df['end_datetime'] - filtered_df['start_datetime']).dt.total_seconds() / 3600  # Convert to hours

# Save the filtered DataFrame to a new CSV file
filtered_df.to_csv(r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/Madrid/csv_output/{event_name}.csv", index=False)

# Display the filtered DataFrame with the duration column
print(filtered_df)

# Calculate the sum of the duration column
total_duration = filtered_df['duration'].sum()

# Create a scatter plot with the date on the x-axis and the duration on the y-axis
plt.figure(figsize=(12, 8))
plt.scatter(filtered_df['start_datetime'], filtered_df['duration'], color='darkblue', alpha=0.6, edgecolors='w', linewidth=0.5)
plt.title(f'{event_name.replace("_", " ").title()} Duration Over Time')
plt.xlabel('Event Date')
plt.ylabel('Duration (hours)')
plt.xticks(rotation=45)
plt.grid(True)

# Add the total duration below the plot
plt.figtext(0.5, -0.05, f'Total Duration: {total_duration:.2f} hours', ha='center', fontsize=12)

# Save the scatter plot to a file
scatter_plot_path = r"/home/ec2-user/SageMaker/Aleksei/Weather_Analysis/Working_analysis/Madrid/csv_output/{event_name}_durations_scatter.png"
plt.savefig(scatter_plot_path, bbox_inches='tight')

# Close the plot to free up memory
plt.close()

print(f"Scatter plot saved to {scatter_plot_path}")
