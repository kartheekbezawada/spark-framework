import os
from PyPDF2 import PdfReader, PdfWriter

# Define the directory path (change 'downloads' if needed)
downloads_folder = r'C:\Users\Aryan\OneDrive\Desktop\New folder'

# Create a PdfWriter object for the merged PDF
pdf_writer = PdfWriter()

# Loop through all files in the downloads folder
for filename in os.listdir(downloads_folder):
  # Check if it's a PDF file
  if filename.endswith('.pdf'):
    # Open the PDF file
    pdf_reader = PdfReader(os.path.join(downloads_folder, filename))
    
    # Extract the first page
    first_page = pdf_reader.pages[0]
    
    # Add the first page to the PdfWriter object
    pdf_writer.addPage(first_page)

# Create the output filename (change 'merged_first_pages.pdf' if needed)
output_filename = 'merged_first_pages.pdf'

# Save the merged PDF file to the downloads folder
with open(os.path.join(downloads_folder, output_filename), 'wb') as output_file:
  pdf_writer.write(output_file)

print(f"Successfully merged first pages of PDFs into {output_filename}")