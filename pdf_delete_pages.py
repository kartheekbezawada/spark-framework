import os
from PyPDF2 import PdfReader, PdfWriter

def delete_pdf_pages(input_pdf_path, pages_to_delete, output_pdf_path):
    # Create a PdfReader object for the input PDF
    pdf_reader = PdfReader(input_pdf_path)
    
    # Create a PdfWriter object for the output PDF
    pdf_writer = PdfWriter()
    
    # Loop through each page in the input PDF
    for page_num in range(len(pdf_reader.pages)):
        # Check if the current page number is in the list of pages to delete
        if page_num not in pages_to_delete:
            # If not, add the current page to the PdfWriter object
            pdf_writer.add_page(pdf_reader.pages[page_num])
    
    # Save the new PDF file with the remaining pages
    with open(output_pdf_path, 'wb') as output_file:
        pdf_writer.write(output_file)

# Define the input PDF file path (change 'merged_all_pages.pdf' if needed)
input_pdf_path = r'C:\Users\Aryan\OneDrive\Anupama ILR\bank statements\merged_all_pages.pdf'

# Define the pages to delete (0-based index)
pages_to_delete = [1,3,5,7,9,12,14,16,18,20,22,24,28,30,33]  

# Define the output PDF file path (change 'remaining_pages.pdf' if needed)
output_pdf_path = r'C:\Users\Aryan\OneDrive\Anupama ILR\bank statements\merged_all_pages.pdf'

# Call the function to delete the specified pages
delete_pdf_pages(input_pdf_path, pages_to_delete, output_pdf_path)

print(f"Successfully deleted specified pages and saved the remaining pages to {output_pdf_path}")