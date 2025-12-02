import os

directory = r"C:\Users\US7K7NY\Downloads\Lake_Nona"
output_file = os.path.join(directory, "combined.txt")

with open(output_file, "w", encoding="utf-8") as outfile:
    for filename in os.listdir(directory):
        if filename.endswith(".txt") and filename != "combined.txt":
            file_path = os.path.join(directory, filename)
            
            with open(file_path, "r", encoding="utf-8") as infile:
                outfile.write(f"--- Start of {filename} ---\n")
                outfile.write(infile.read())
                outfile.write(f"\n--- End of {filename} ---\n\n")

print("Merging complete! File saved as combined.txt")
