import csv

# Constants
ELECTION = "2024 GENERAL"
STATE = "PENNSYLVANIA"
COUNTY = "CUMBERLAND COUNTY"
SOURCE_FILENAME = "cumberland_cleaned.txt"
SOURCE_URL = "https://www.cumberlandcountypa.gov/DocumentCenter/View/52475/Official-Precinct-Report"
RESULT_STATUS = "OFFICIAL"
DATETIME_RETRIEVED = "11/19/2024 08:53AM"

# File paths
input_filename = "cumberland_cleaned.txt"
output_filename = "cumberland_parsed.csv"
unmatched_filename = "cumberland_unmatched.txt"

with open(input_filename, "r") as file:
    lines = file.readlines()

# Parsing and processing
rows = []
unmatched_lines = []  # To collect unmatched lines
current_precinct = None
current_office = None

for index, line in enumerate(lines):
    line = line.strip()
    if not line:  # Skip blank lines
        continue

    # Handle page break and take the next line as the precinct name
    if line == "-PAGE-BREAK-":
        current_precinct = lines[index +
                                 1].strip() if index + 1 < len(lines) else None
        current_office = None  # Reset office for the new precinct
        continue

    # Split the line into parts
    parts = line.split()

    # Check if the last four parts are numeric (vote counts)
    if len(parts) >= 5 and all(part.isdigit() for part in parts[-4:]):
        try:
            party = parts[0]  # First part is the party
            # Everything between the party and the vote counts is the candidate name
            candidate_name = " ".join(parts[1:-4])
            votes_total, votes_election_day, votes_mail, votes_provisional = map(
                int, parts[-4:])  # Last four parts are vote counts

            # Manually update the write-in lines, because they parse wrong.
            if (candidate_name == "Totals" and party == "Write-in"):
                candidate_name = "WRITE-IN"
                party = ""

            # Append processed data to rows
            vote_modes = [
                ("Total", votes_total),
                ("Election Day", votes_election_day),
                ("Mail", votes_mail),
                ("Provisional", votes_provisional),
            ]
            for vote_mode, votes in vote_modes:
                if vote_mode == "Total":
                    continue  # Skip "Total" rows

                rows.append({
                    "election": ELECTION,
                    "state": STATE,
                    "county": COUNTY,
                    "precinct": current_precinct,
                    "office": current_office,
                    "candidate": candidate_name,
                    "party": party,
                    "vote_mode": vote_mode,
                    "votes": votes,
                    "writein": "yes" if candidate_name.lower() == "write-in totals" else "no",
                    "result_status": RESULT_STATUS,
                    "source_url": SOURCE_URL,
                    "source_filename": SOURCE_FILENAME,
                    "datetime_retrieved": DATETIME_RETRIEVED,
                })
        except ValueError:
            unmatched_lines.append(line)  # Log lines that failed processing

    # If it doesn't match the data row shape, treat it as an office title
    elif current_precinct and line.isupper():
        current_office = line

    # Otherwise, log unmatched lines
    else:
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
