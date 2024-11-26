import csv

# Constants
ELECTION = "2024 GENERAL"
STATE = "PENNSYLVANIA"
COUNTY = "DAUPHIN COUNTY"
SOURCE_FILENAME = "dauphin_data.txt"
RESULT_STATUS = "PRELIMINARY"
DATETIME_RETRIEVED = "11/25/2024 12:36"
JURISDICTION = "DAUPHIN COUNTY"

# Party lookup table
PARTY_LOOKUP = {
    # PRESIDENT OF THE UNITED STATES
    "KAMALA D HARRIS": "DEM",
    "DONALD J TRUMP": "REP",
    "CHASE OLIVER": "LIBRT",
    "JILL STEIN": "GREEN",
    # UNITED STATES SENATOR
    "ROBERT P CASEY JR": "DEM",
    "DAVE MCCORMICK": "REP",
    "JOHN C THOMAS": "LIBRT",
    "LEILA HAZOU": "GREEN",
    "MARTY SELKER": "CONSTITUTION",
    # ATTORNEY GENERAL
    "EUGENE DEPASQUALE": "DEM",
    "DAVE SUNDAY": "REP",
    "ROBERT COWBURN": "LIBRT",
    "RICHARD L WEISS": "GREEN",
    "JUSTIN L MAGILL": "CONSTITUTION",
    "ERIC L SETTLE": "FORWARD",
    # AUDITOR GENERAL
    "MALCOLM KENYATTA": "DEM",
    "TIM DEFOOR": "REP",
    "REECE SMITH": "LIBRT",
    "ERIC K ANTON": "AMERICAN SOLIDARITY",
    "BOB GOODRICH": "CONSTITUTION",
    # STATE TREASURER
    "ERIN MCCLELLAND": "DEM",
    "STACY GARRITY": "REP",
    "NICKOLAS CIESIELSKI": "LIBRT",
    "TROY BOWMAN": "CONSTITUTION",
    "CHRIS FOSTER": "FOWARD",
    # REPRESENTATIVE IN CONGRESS
    "JANELLE STELSON": "DEM",
    "SCOTT PERRY": "REP",
    # SENATOR IN THE GENERAL ASSEMBLY - SD15
    "PATTY KIM": "DEM",
    "NICK DIFRANCESCO": "REP",
    # REPRESENTATIVE IN THE GENERAL ASSEMBLY - HD103
    "NATE DAVIDSON": "DEM",
    "CINDI WARD": "REP",
    # REPRESENTATIVE IN THE GENERAL ASSEMBLY - HD104
    "DAVE MADSEN": "DEM",
    # REPRESENTATIVE IN THE GENERAL ASSEMBLY - HD105
    "JUSTIN C FLEMING": "DEM",
    # REPRESENTATIVE IN THE GENERAL ASSEMBLY - HD106
    "ANJU SINGH": "DEM",
    "TOM MEHAFFIE": "REP",
    # REPRESENTATIVE IN THE GENERAL ASSEMBLY - HD125
    "GENE STILP": "DEM",
    "JOE KERWIN": "REP",
    # PENBROOK BREWERY LICENSES
    # SUSQUEHANNA TOWNSHIP SCHOOL DISTRICT TAX REFERENDUM
}


# Load input text from file
input_filename = "./dauphin_data.txt"
output_filename = "dauphin_parsed.csv"

with open(input_filename, "r") as file:
    lines = file.readlines()

# Parsing and processing
rows = []
unmatched_lines = []  # To collect lines that don't match
current_precinct = None
current_office = None
current_source_url = None

# Track printed offices and candidates -- this is used mostly to help me generate the party lookup table below
# printed_offices = set()
# printed_candidates = set()


for line in lines:
    line = line.strip()
    if not line:  # Skip blank lines
        continue
    if line.startswith("http://") or line.startswith("https://"):
        # Source URL line
        current_source_url = line
    elif "Machine" in line and "Mail-in" in line:
        # Skip headers like "Machine Mail-in Provisional Total"
        continue
    else:
        # Candidate rows: Split line into candidate and votes
        parts = line.split()
        if len(parts) >= 5 and all(part.isdigit() for part in parts[-4:]):
            try:
                candidate_name = " ".join(parts[:-4])
                votes_machine, votes_mail, votes_provisional, votes_total = map(
                    int, parts[-4:])

                # # Print the office if it hasn't been printed yet
                # if current_office and current_office not in printed_offices:
                #     print(f"# {current_office}")
                #     printed_offices.add(current_office)

                # # Print the candidate and their party if they haven't been printed yet
                # if candidate_name not in printed_candidates and (candidate_name != "WRITE-IN" and candidate_name != "YES" and candidate_name != "NO"):
                #     party = PARTY_LOOKUP.get(candidate_name, "")
                #     print(f"\"{candidate_name}\" : \"{party}\",")
                #     printed_candidates.add(candidate_name)

                # Append processed data to rows
                vote_modes = [("Machine", votes_machine), ("Mail-in",
                                                           votes_mail), ("Provisional", votes_provisional)]
                for vote_mode, votes in vote_modes:
                    rows.append({
                        "election": ELECTION,
                        "state": STATE,
                        "county": COUNTY,
                        "precinct": current_precinct,
                        "jurisdiction": JURISDICTION,
                        "office": current_office,
                        "candidate": candidate_name,
                        "party": PARTY_LOOKUP.get(candidate_name, ""),
                        "vote_mode": vote_mode,
                        "votes": votes,
                        "writein": "yes" if candidate_name == "WRITE-IN" else "no",
                        "result_status": RESULT_STATUS,
                        "source_url": current_source_url,
                        "source_filename": SOURCE_FILENAME,
                        "datetime_retrieved": DATETIME_RETRIEVED,
                    })
            except ValueError:
                # Log lines that failed processing
                unmatched_lines.append(line)
        elif line.isupper():
            # Office title
            current_office = line
        elif current_office:
            # Precinct
            current_precinct = line
        else:
            unmatched_lines.append(line)  # Log lines that failed processing

# Output unmatched lines for review
if unmatched_lines:
    print("Unmatched Lines:")
    for unmatched_line in unmatched_lines:
        print(unmatched_line)


# Write to CSV
csv_columns = [
    "election", "state", "county", "precinct", "jurisdiction", "office",
    "candidate", "party", "vote_mode", "votes", "writein",
    "result_status", "source_url", "source_filename", "datetime_retrieved"
]

with open(output_filename, "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
    writer.writeheader()
    writer.writerows(rows)

print(f"Results written to {output_filename}")
