import fitz  # PyMuPDF
from PyPDF2 import PdfReader, PdfWriter
import os

# Configuration Constants
INPUT_PDF = "./StatementOfVotesCastRPT__reduced__.pdf"
OUTPUT_DIRECTORY = "split_sections"  # Directory to save the split PDFs
# Keyword to search for in the PDF. this finds the office titles since they each say something like (Vote for 1)
KEYWORD = "Vote for"

# Open the PDF with fitz for text extraction
doc = fitz.open(INPUT_PDF)
split_pages = []
titles = []

# Scan each page for the keyword
for page_num in range(len(doc)):
    page = doc[page_num]
    text = page.get_text("text")

    if KEYWORD in text:
        # Extract the title (text before "Vote for")
        lines = text.splitlines()
        for line in lines:
            if KEYWORD in line:
                title = line.split(KEYWORD)[0].strip()
                titles.append(title)
                split_pages.append(page_num)
                break

split_pages.append(len(doc))  # Add the last page as the endpoint

# Split and save the sections
reader = PdfReader(INPUT_PDF)
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

for i in range(len(split_pages) - 1):
    start_page = split_pages[i]
    end_page = split_pages[i + 1]  # End is exclusive
    title = titles[i]
    sanitized_title = (
        "".join(c if c.isalnum() or c in " _-" else "_" for c in title)
        .replace(" ", "_")  # Replace spaces with underscores
        .replace("__", "_")  # Remove double underscores
        .strip("_")  # Remove leading and trailing underscores
    )
    # Write the split section to a new PDF
    writer = PdfWriter()
    for page_num in range(start_page, end_page):
        writer.add_page(reader.pages[page_num])

    output_path = os.path.join(
        OUTPUT_DIRECTORY, f"{sanitized_title or 'Untitled'}.pdf")
    with open(output_path, "wb") as output_file:
        writer.write(output_file)
    print(f"Saved: {output_path}")

print("Splitting complete.")
