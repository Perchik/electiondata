import fitz  # PyMuPDF
from PyPDF2 import PdfReader, PdfWriter
import os

# Configuration Constants
INPUT_PDF = "federal_offices.pdf"
OUTPUT_DIRECTORY = "split_races"  # Directory to save the split PDFs
# Header information to skip when parsing
HEADER_KEYWORDS = [
    "Wayne County, Michigan",
    "20241105 Wayne General Election",
    "Precinct Canvass",
    "November 5, 2024",
]

# Open the PDF with fitz for text extraction
doc = fitz.open(INPUT_PDF)
split_pages = {}
current_race = None

# Process each page
for page_num in range(len(doc)):
    page = doc[page_num]
    text = page.get_text("text")
    lines = text.splitlines()

    # Skip header lines
    lines = [line for line in lines if line not in HEADER_KEYWORDS]

    # Identify race name, accounting for wrapped text and hyphenated splits
    race_name_parts = []
    is_collecting_race = False

    for line in lines:
        # Race lines start with "1 " and may continue on subsequent lines
        if line.startswith("1 "):  # New race starts
            if race_name_parts:  # Save previous race if still collecting
                race_name = " ".join(race_name_parts).strip()
                current_race = race_name
                if current_race not in split_pages:
                    split_pages[current_race] = []
                split_pages[current_race].append(page_num)
                race_name_parts = []  # Reset for the next race
            is_collecting_race = True
            # Remove leading "1"
            race_name_parts.append(" ".join(line.split()[1:]))
        elif is_collecting_race and not line.startswith("1 ") and not line[0].isdigit():
            # Continuation of the race name (no numbers at the start)
            if race_name_parts and race_name_parts[-1].endswith("-"):
                # Handle hyphenated split, keeping the hyphen. This happens when a page wraps on "vice-president"
                race_name_parts[-1] = race_name_parts[-1] + line.strip()
            else:
                race_name_parts.append(line.strip())
        else:
            is_collecting_race = False

    # Save the last collected race name on the page
    if race_name_parts:
        race_name = " ".join(race_name_parts).strip()
        current_race = race_name
        if current_race not in split_pages:
            split_pages[current_race] = []
        split_pages[current_race].append(page_num)

# Create output directory
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

# Split and save the sections
reader = PdfReader(INPUT_PDF)

for race, pages in split_pages.items():
    sanitized_race_name = (
        "".join(c if c.isalnum() or c in " _-" else "_" for c in race)
        .replace(" ", "_")  # Replace spaces with underscores
        .replace("__", "_")  # Remove double underscores
        .strip("_")  # Remove leading and trailing underscores
    )

    # Write the split section to a new PDF
    writer = PdfWriter()
    for page_num in pages:
        writer.add_page(reader.pages[page_num])

    output_path = os.path.join(
        OUTPUT_DIRECTORY, f"{sanitized_race_name or 'Untitled'}.pdf"
    )
    with open(output_path, "wb") as output_file:
        writer.write(output_file)
    print(f"Saved: {output_path}")

print("Splitting complete.")
