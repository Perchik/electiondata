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


def create_directories(race_name):
    """Create directories for a specific race."""
    race_dir = os.path.join(RACES_DIRECTORY, race_name)
    parsed_dir = os.path.join(race_dir, "parsed")
    skipped_dir = os.path.join(race_dir, "skipped")
    os.makedirs(parsed_dir, exist_ok=True)
    os.makedirs(skipped_dir, exist_ok=True)
    return parsed_dir, skipped_dir


def transform_table_precinct(df):
    """
    Transforms a precinct-style table into long format.
    - Dynamically detects precinct names.
    - Processes all candidate columns except blanks and 'Total Votes'.
    """
    transformed_data = []
    current_precinct = None
    valid_methods = {"Mail-in", "Provisional", "Election Day", "Total"}
    special_values = {"County", "PA County"}

    # Fix headers if the first row contains candidate names
    if isinstance(df.columns[0], int):  # If headers are numeric
        df.columns = df.iloc[0]  # Set headers to the first row
        df = df[1:]  # Remove the first row from the data

    for _, row in df.iterrows():
        first_col = row.iloc[0]  # First column of the current row

        # Detect precinct name
        if first_col not in valid_methods and first_col not in special_values:
            current_precinct = first_col
            continue  # Move to the next row to process methods for this precinct

        # Process rows containing voting methods
        if first_col in valid_methods:
            method = first_col
            # Skip the method column
            for candidate, votes in zip(df.columns[1:], row.iloc[1:]):
                if not isinstance(candidate, str) or candidate.strip() == "" or candidate.strip() == "Total Votes":
                    continue
                candidate = candidate.replace("\n", " ").strip()
                transformed_data.append({
                    "precinct": current_precinct,
                    "method": method,
                    "candidate": candidate,
                    "votes": votes
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
        if EXTRA_LOGGING:
            print(f"  Found {len(tables)} tables on {
                  base_name}, page {page_num}.")

        if len(tables) == 0:
            print(f"  No tables found on {base_name}, page {page_num}.")
            return
        # Check for tables with "Times Cast" and filter them out
        valid_tables = []
        for i, table in enumerate(tables):
            if "Times Cast" in table.df.to_string():
                # Save the skipped table
                table.df.to_csv(f"{skipped_file}_table_{
                                i + 1}.csv", index=False)
            else:
                valid_tables.append(table)

        # If no valid tables remain, save the entire page's data and skip
        if not valid_tables:
            print(f"All tables on {base_name}, page {
                  page_num} were skipped. Saving to skipped_pages.")
            for i, table in enumerate(tables):
                table.df.to_csv(f"{skipped_file}_table_{
                                i + 1}.csv", index=False)
            return

        # Process the remaining valid table(s)
        for table in valid_tables:
            # Convert the table to a DataFrame
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
    """Merge all CSVs in the parsed directory into a single CSV."""
    csv_files = [os.path.join(parsed_dir, f)
                 for f in os.listdir(parsed_dir) if f.endswith(".csv")]
    if not csv_files:
        print(f"  No parsed CSV files to merge in {parsed_dir}.")
        return

    merged_df = pd.concat((pd.read_csv(f)
                          for f in csv_files), ignore_index=True)
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
