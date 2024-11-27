import os
import csv
import pandas as pd
import re

# Constants
ELECTION = "2024 GENERAL"
STATE = "PENNSYLVANIA"
COUNTY = "MONTGOMERY COUNTY"
SOURCE_URL = "https://www.montgomerycountypa.gov/DocumentCenter/View/45586/2024UnofficialGeneralElectionStatementofVotesCast?bidId="
RESULT_STATUS = "UNOFFICIAL"
DATETIME_RETRIEVED = "11/19/2024 3:35:01 PM"

# Directory paths
RACES_DIRECTORY = "races"
OUTPUT_FILENAME = "parsed_results.csv"
UNMATCHED_FILENAME = "unmatched_lines.txt"

# Define office importance ranking
OFFICE_RANKING = {
    "PRESIDENTIAL ELECTORS": 1,
    "UNITED STATES SENATOR": 2,
    "REPRESENTATIVE IN CONGRESS": 3,
    "ATTORNEY GENERAL": 4,
    "AUDITOR GENERAL": 5,
    "SENATOR IN THE GENERAL ASSEMBLY": 6,
}

# Helper function to determine office rank


def get_office_rank(office):
    for key, rank in OFFICE_RANKING.items():
        if office.startswith(key):
            return rank
    return float("inf")  # Default rank for unlisted offices


# Collect unmatched lines for logging
unmatched_lines = []

# Process each race-level CSV
rows = []

for race_dir in os.listdir(RACES_DIRECTORY):
    race_path = os.path.join(RACES_DIRECTORY, race_dir)
    if not os.path.isdir(race_path):  # Skip non-directories
        continue

    race_csv = os.path.join(race_path, f"{race_dir.lower()}.csv")
    if not os.path.exists(race_csv):
        print(f"Skipping {race_csv}: Race-level CSV not found.")
        continue

    # Determine the office title from the race directory
    office_title = race_dir.replace("_", " ").upper()
    print(f"Processing office: {office_title}")

    try:
        # Read the race-level CSV
        df = pd.read_csv(race_csv)

        for _, row in df.iterrows():
            try:
                # Extract necessary fields
                precinct = row.get("precinct", "").strip()
                candidate = row.get("candidate", "").upper().strip()
                party = row.get("party", "").strip()
                votes = row.get("votes", 0)
                vote_mode = row.get("method", "").strip()

                # Handle Unresolved Write-In
                if candidate.lower() == "unresolved write-in":
                    candidate = "(Other)"
                    party = "Unresolved Write-In"
                    is_writein = "yes"

                else:
                    # Extract party name from candidate if in parentheses
                    match = re.search(r"\(([^)]+)\)", candidate)
                    if match:
                        party = match.group(1).strip()
                        candidate = re.sub(
                            r"\s*\([^)]*\)", "", candidate).strip()

                    # Detect write-ins based on party name
                    is_writein = "yes" if "write" in party.lower() else "no"

                # Append processed data to rows
                rows.append({
                    "election": ELECTION,
                    "state": STATE,
                    "county": COUNTY,
                    "precinct": precinct,
                    "office": office_title,
                    "candidate": candidate,
                    "party": party,
                    "vote_mode": vote_mode,
                    "votes": votes,
                    "writein": is_writein,
                    "result_status": RESULT_STATUS,
                    "source_url": SOURCE_URL,
                    "source_filename": race_csv,
                    "datetime_retrieved": DATETIME_RETRIEVED,
                })
            except Exception as e:
                unmatched_lines.append(
                    f"Error processing row in {race_csv}: {e}")

    except Exception as e:
        unmatched_lines.append(f"Error reading {race_csv}: {e}")

# Sort rows by office ranking
rows = sorted(rows, key=lambda x: get_office_rank(x["office"]))

# Write unmatched lines to a separate file
if unmatched_lines:
    with open(UNMATCHED_FILENAME, "w") as unmatched_file:
        unmatched_file.write("\n".join(unmatched_lines))
    print(f"Unmatched lines written to {UNMATCHED_FILENAME}")

# Write processed rows to CSV
csv_columns = [
    "election", "state", "county", "precinct", "office",
    "candidate", "party", "vote_mode", "votes", "writein",
    "result_status", "source_url", "source_filename", "datetime_retrieved"
]

with open(OUTPUT_FILENAME, "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
    writer.writeheader()
    writer.writerows(rows)

print(f"Results written to {OUTPUT_FILENAME}")
