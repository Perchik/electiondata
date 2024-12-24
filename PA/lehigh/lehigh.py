import csv

# Constants
ELECTION = "2024 GENERAL"
STATE = "PENNSYLVANIA"
COUNTY = "LEHIGH COUNTY"
SOURCE_FILENAME = "precincts_8.csv"
SOURCE_URL = "https://www.livevoterturnout.com/ENR/lehighpaenr/8/en/Index_8.html"
RESULT_STATUS = "OFFICIAL"
DATETIME_RETRIEVED = "12/7/2024 10:39PM"

# File paths
input_filename = "precincts_8.csv"
output_filename = "lehigh_parsed.csv"
unmatched_filename = "lehigh_unmatched.txt"

with open(input_filename, "r") as file:
    lines = file.readlines()

# Parsing and processing
rows = []
unmatched_lines = []  # To collect unmatched lines

for line in lines:
    line = line.strip()
    if not line:  # Skip blank lines
        continue

    # Split the line by commas, removing any surrounding quotes
    parts = [part.strip().strip('"') for part in line.split(",")]

    # Expecting the format: Precinct, Contest Name, Candidate Name, Votes, Voter Turnout
    if len(parts) == 5:
        try:
            precinct, office, candidate_raw, votes, voter_turnout = parts

            # Extract party from the first three characters of the candidate string
            party = candidate_raw[:3]
            # Remove the party prefix and trim the remaining name
            candidate_name = candidate_raw[4:]

            # Convert votes to integer
            votes = int(votes)

            # Append processed data to rows
            rows.append({
                "election": ELECTION,
                "state": STATE,
                "county": COUNTY,
                "precinct": precinct,
                "office": office,
                "candidate": candidate_name,
                "party": party,
                "vote_mode": "Total",  # Only one vote mode
                "votes": votes,
                "writein":"no",
                "result_status": RESULT_STATUS,
                "source_url": SOURCE_URL,
                "source_filename": SOURCE_FILENAME,
                "datetime_retrieved": DATETIME_RETRIEVED,
            })
        except ValueError:
            unmatched_lines.append(line)  # Log lines that failed processing
    else:
        # Log lines that don't match the expected format
        unmatched_lines.append(line)

# Write unmatched lines to a separate file
if unmatched_lines:
    with open(unmatched_filename, "w") as unmatched_file:
        unmatched_file.write("\n".join(unmatched_lines))
    print(f"Unmatched lines written to {unmatched_filename}")

# Write processed rows to CSV
csv_columns = [
    "election", "state", "county", "precinct", "office",
    "candidate", "party", "vote_mode", "votes", "writein",
    "result_status", "source_url", "source_filename", "datetime_retrieved"
]

with open(output_filename, "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
    writer.writeheader()
    writer.writerows(rows)

print(f"Results written to {output_filename}")
