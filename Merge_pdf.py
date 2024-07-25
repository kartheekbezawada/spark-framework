import os
from PyPDF2 import PdfReader, PdfWriter

# Define the directory path (change 'downloads' if needed)
downloads_folder = r'C:\Users\Aryan\OneDrive\Anupama ILR\Combining'

# Create a PdfWriter object for the merged PDF
pdf_writer = PdfWriter()

# Loop through all files in the downloads folder
for filename in os.listdir(downloads_folder):
  # Check if it's a PDF file
  if filename.endswith('.pdf'):
    # Open the PDF file
    pdf_reader = PdfReader(os.path.join(downloads_folder, filename))
    
    # Get the total number of pages in the current PDF
    num_pages = len(pdf_reader.pages)
    
    # Loop through each page in the current PDF
    for page_num in range(num_pages):
      # Extract the current page
      current_page = pdf_reader.pages[page_num]
      
      # Add the current page to the PdfWriter object
      pdf_writer.add_page(current_page)

# Create the output filename (change 'merged_all_pages.pdf' if needed)
output_filename = 'merged_all_pages.pdf'

# Save the merged PDF file to the downloads folder
with open(os.path.join(downloads_folder, output_filename), 'wb') as output_file:
  pdf_writer.write(output_file)

print(f"Successfully merged all pages of PDFs into {output_filename}")