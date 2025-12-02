import re
import csv

# Function to parse the text data
def parse_hail_data(text):
    # Regular expression pattern to match each event entry reminder to myself adjust according to what I need
    pattern = re.compile(r'--- (.*?) event_visibility all Orlando ---\nlow_val=(.*?),\s*low_threshold=(.*?)\nhigh_val=(.*?),\s*high_threshold=(.*?)\n')
    
    # Find all matches in the text
    matches = pattern.findall(text)
    
    # Create a list of dictionaries to store structured data
    data = []
    for match in matches:
        event = {
            'timestamp': match[0],
            'low_val': match[1],
            'low_threshold': match[2],
            'high_val': match[3],
            'high_threshold': match[4]
        }
        data.append(event)
    
    return data

# Function to write data to a CSV file
def write_to_csv(data, filename):
    # Define the header
    header = ['timestamp', 'low_val', 'low_threshold', 'high_val', 'high_threshold']
    
    # Write data to CSV
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=header)
        writer.writeheader()
        writer.writerows(data)

# Read the input text file
input_file = "C:\\Users\\US7K7NY\\Downloads\\Lake_Nona\\combined.txt"
with open(input_file, 'r') as file:
    text = file.read()

# Parse the data
parsed_data = parse_hail_data(text)

# Write the data to a CSV file
output_file = "C:\\Users\\US7K7NY\\Downloads\\Lake_Nona\\event_visibility.csv"
write_to_csv(parsed_data, output_file)

print(f'Data has been written to {output_file}')