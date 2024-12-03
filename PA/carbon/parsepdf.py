import re
import os
import random
import camelot
import pandas as pd
from PyPDF2 import PdfReader, PdfWriter
from multiprocessing import Pool

# Configuration Constants
INPUT_DIRECTORY = "split_sections"  # Directory containing split PDFs
RACES_DIRECTORY = "races"  # Top-level directory for race-specific folders
NUM_WORKERS = 4  # Number of parallel processes
DEBUG_MODE = False
EXTRA_LOGGING = False


def sanitize_string(value):
    """
    Sanitizes a string by:
    - Removing extra newlines and carriage returns.
    - Stripping leading and trailing whitespace.
    - Replacing multiple spaces or tabs with a single space.
    """
    if not isinstance(value, str):
        return value  # If it's not a string, return as is
    return " ".join(value.replace("\n", " ").replace("\r", " ").split())


def create_directories(race_name):
    """Create directories for a specific race."""
    race_dir = os.path.join(RACES_DIRECTORY, race_name)
    parsed_dir = os.path.join(race_dir, "parsed")
    skipped_dir = os.path.join(race_dir, "skipped")
    os.makedirs(parsed_dir, exist_ok=True)
    os.makedirs(skipped_dir, exist_ok=True)
    return parsed_dir, skipped_dir


def clean_candidate_name(candidate):
    """
    Cleans and sanitizes the candidate name by:
    - Removing everything between the slash (/) and the opening parenthesis (if present).
    - Stripping leading/trailing whitespace.
    - Replacing multiple spaces or newlines with a single space.
    Example:
        "KAMALA D HARRIS / TIM WALZ (DEM)" -> "KAMALA D HARRIS (DEM)"
        "DONALD J TRUMP (REP)" -> "DONALD J TRUMP (REP)"
    """
    if not isinstance(candidate, str):
        return candidate  # If it's not a string, return as is

    candidate = sanitize_string(candidate)
    # Remove text between '/' and '(' if both are present
    candidate = re.sub(r'/[^()]*\(', '(', candidate)

    return candidate.strip()


def transform_table_precinct(df):
    """
    Transforms a precinct-style table where each row contains a precinct name
    followed by columns for each candidate's votes.
    """
    transformed_data = []

    # Fix headers if needed
    if isinstance(df.columns[0], int):  # If headers are numeric
        df.columns = df.iloc[0]  # Set headers to the first row
        df = df[1:]  # Remove the first row from the data

    # Iterate through rows
    for _, row in df.iterrows():
        # Precinct name is the first column
        precinct = sanitize_string(row.iloc[0])

       # Skip rows with unwanted precincts (case-insensitive)
        if any(word in precinct.lower() for word in ["total", "cumulative", "carbon"]):
            continue

        # Process columns in pairs: candidate name and votes
        for i in range(1, len(row), 2):  # Step by 2 (name, votes)
            if i >= len(df.columns):  # Prevent index overflow
                break
            # Candidate name is in the header
            candidate = clean_candidate_name(df.columns[i])
            votes = row.iloc[i]  # Votes are in the same row

            # Skip empty or irrelevant candidates
            if candidate.lower() == "total votes" or pd.isna(candidate) or pd.isna(votes):
                continue

            transformed_data.append({
                "precinct": precinct,
                "candidate": candidate.strip(),
                "votes": int(votes) if str(votes).isdigit() else 0
            })

    # Convert to DataFrame
    return pd.DataFrame(transformed_data)


def process_page(pdf_path, page_num, parsed_dir, skipped_dir):
    """Processes a single page."""
    page_str = str(page_num)
    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    output_file = os.path.join(parsed_dir, f"{base_name}_page_{page_num}.csv")
    skipped_file = os.path.join(
        skipped_dir, f"{base_name}_page_{page_num}.csv")

    print(f"Processing {base_name}, page {page_num}...")

    if os.path.exists(output_file) or os.path.exists(skipped_file):
        print(f"  Skipping {base_name}, page {page_num}: already processed.")
        return

    try:
        tables = camelot.read_pdf(pdf_path, pages=page_str, flavor="lattice")
        if len(tables) == 0:
            print(f"  No tables found on {base_name}, page {page_num}.")
            return

        # Process the first table found on the page
        table = tables[0]  # Assuming one table per page
        df = table.df

        # Transform the table
        transformed_df = transform_table_precinct(df)

        # Save the transformed table to a CSV
        transformed_df.to_csv(output_file, index=False)
    except Exception as e:
        print(f"  Error processing {base_name}, page {page_num}: {e}")
        with open(f"{skipped_file}_error.txt", "w") as error_file:
            error_file.write(str(e))


def process_pdf(pdf_file):
    """Processes all pages of a PDF file."""
    pdf_path = os.path.join(INPUT_DIRECTORY, pdf_file)
    race_name = os.path.splitext(pdf_file)[0]

    parsed_dir, skipped_dir = create_directories(race_name)

    try:
        reader = PdfReader(pdf_path)
        total_pages = len(reader.pages)

        print(f"Processing {pdf_file} with {total_pages} pages...")
        for page_num in range(1, total_pages + 1):
            process_page(pdf_path, page_num, parsed_dir, skipped_dir)

        output_file = os.path.join(RACES_DIRECTORY, race_name, f"{
                                   race_name.lower()}.csv")
        merge_parsed_csvs(parsed_dir, output_file)
        print(f"  Merged all parsed tables into {output_file}.")
    except Exception as e:
        print(f"Error processing {pdf_file}: {e}")


def merge_parsed_csvs(parsed_dir, output_file):
    """
    Merge all CSVs in the parsed directory into a single CSV and sort it.
    """
    csv_files = [os.path.join(parsed_dir, f)
                 for f in os.listdir(parsed_dir) if f.endswith(".csv")]
    if not csv_files:
        print(f"  No parsed CSV files to merge in {parsed_dir}.")
        return

    # Read and concatenate all CSV files
    merged_df = pd.concat((pd.read_csv(f)
                          for f in csv_files), ignore_index=True)

    # Sort the DataFrame by precinct and candidate columns (adjust column names as needed)
    if "precinct" in merged_df.columns and "candidate" in merged_df.columns:
        merged_df = merged_df.sort_values(by=["precinct", "candidate"])

    # Save the sorted DataFrame to a CSV file
    merged_df.to_csv(output_file, index=False)


if __name__ == '__main__':
    os.makedirs(RACES_DIRECTORY, exist_ok=True)

    pdf_files = [f for f in os.listdir(INPUT_DIRECTORY) if f.endswith(".pdf")]
    if DEBUG_MODE:
        debug_file = "PRESIDENTIAL_ELECTORS.pdf"
        pdf_path = os.path.join(INPUT_DIRECTORY, debug_file)

        if not os.path.exists(pdf_path):
            print(f"Error: {debug_file} does not exist in the {
                INPUT_DIRECTORY} directory.")
        else:
            print(f"Debug mode: Using {
                debug_file} for debugging on pages 46 and 47.")
            debug_pages = [46, 47]

            try:
                reader = PdfReader(pdf_path)
                total_pages = len(reader.pages)

                for page_num in debug_pages:
                    if page_num > total_pages:
                        print(f"  Page {page_num} does not exist in {
                            debug_file}. Skipping...")
                        continue

                    print(f"  Processing page {page_num} from {debug_file}.")

                    # Export the selected page as its own PDF
                    debug_output_dir = os.path.join(
                        RACES_DIRECTORY, "debug_pages")
                    os.makedirs(debug_output_dir, exist_ok=True)
                    debug_pdf_path = os.path.join(
                        debug_output_dir, f"{os.path.splitext(debug_file)[0]}_page_{page_num}.pdf")

                    with open(debug_pdf_path, "wb") as output_pdf:
                        writer = PdfWriter()
                        writer.add_page(reader.pages[page_num - 1])
                        writer.write(output_pdf)

                    print(f"  Exported page {page_num} to {debug_pdf_path}.")

                    # Extract and process tables from the page
                    tables = camelot.read_pdf(
                        pdf_path, pages=str(page_num), flavor="lattice")
                    print(f"  Found {len(tables)} tables on page {page_num}.")

                    race_name = os.path.splitext(debug_file)[0]
                    parsed_dir, skipped_dir = create_directories(race_name)

                    for i, table in enumerate(tables):
                        output_file = os.path.join(parsed_dir, f"{race_name}_page_{
                            page_num}_table_{i + 1}.csv")
                        print(f"  Processing table {
                              i + 1} from page {page_num}.")

                        try:
                            # Convert the table to a DataFrame
                            df = table.df
                            if df.empty:
                                print(f"    Table {
                                      i + 1} is empty. Skipping...")
                                continue

                            # Transform and save the table
                            transformed_df = transform_table_precinct(df)
                            if transformed_df.empty:
                                print(f"    Transformed table {
                                    i + 1} is empty. Skipping...")
                                continue

                            transformed_df.to_csv(output_file, index=False)
                            print(f"    Saved transformed table {
                                i + 1} to {output_file}.")
                        except Exception as e:
                            print(f"    Error processing table {
                                i + 1} on page {page_num}: {e}")
                            skipped_file = os.path.join(skipped_dir, f"{race_name}_page_{
                                                        page_num}_table_{i + 1}_error.txt")
                            with open(skipped_file, "w") as error_file:
                                error_file.write(str(e))
            except Exception as e:
                print(f"  Error processing {debug_file}: {e}")
    else:
        # Process all files in parallel
        with Pool(NUM_WORKERS) as pool:
            pool.map(process_pdf, pdf_files)

    print("Table extraction complete.")
