import re

# File paths
input_filename = "cumberland_data.txt"
output_filename = "cumberland_cleaned.txt"

with open(input_filename, "r") as file:
    lines = file.readlines()

# Preprocessing steps
cleaned_lines = []

# List of line prefixes to skip
skip_prefixes = [
    "Precinct Results Report",
    "2024 GENERAL ELECTION",
    "Result Book - Precinct Report -",
    "STATISTICS",
    "Registered Voters",
    "Ballots Cast",
    "Voter Turnout",
    "TOTAL",
    "Day Mail",
    "Vote For"
]

for index, line in enumerate(lines):
    line = line.strip()

    # Skip lines that start with any prefix in skip_prefixes
    if any(line.startswith(prefix) for prefix in skip_prefixes):
        continue
    if line.startswith("November 5, 2024 Cumberland County"):
        cleaned_lines.append("-PAGE-BREAK-")
        continue

    # Remove commas from numbers (e.g., 1,235 -> 1235)
    line = re.sub(r'(\d),(\d)', r'\1\2', line)

    # Add cleaned line
    if line:  # Avoid empty lines
        cleaned_lines.append(line)

# Write the cleaned data to the output file
with open(output_filename, "w") as output_file:
    output_file.write("\n".join(cleaned_lines))

print(f"Cleaned data written to {output_filename}")
