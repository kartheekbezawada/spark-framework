from PyPDF2 import PdfReader, PdfWriter

def extract_pdf_page(input_pdf_path, page_to_extract, output_pdf_path):
    # Create a PdfReader object for the input PDF
    pdf_reader = PdfReader(input_pdf_path)
    
    # Create a PdfWriter object for the output PDF
    pdf_writer = PdfWriter()
    
    # Add the specified page to the PdfWriter object
    pdf_writer.add_page(pdf_reader.pages[page_to_extract])
    
    # Save the new PDF file with the extracted page
    with open(output_pdf_path, 'wb') as output_file:
        pdf_writer.write(output_file)

# Define the input PDF file path (change 'merged_all_pages.pdf' if needed)
input_pdf_path = r'C:/Users/Aryan/OneDrive/Anupama ILR/Combining/Marriage_Pics.pdf'

# Define the page to extract (0-based index)
page_to_extract = 1  # Extracts the 6th page (index 5)

# Define the output PDF file path (change 'extracted_page.pdf' if needed)
output_pdf_path = r'C:/Users/Aryan/OneDrive/Anupama ILR/Combining/Marriage_Pics2.pdf'

# Call the function to extract the specified page
extract_pdf_page(input_pdf_path, page_to_extract, output_pdf_path)

print(f"Successfully extracted page {page_to_extract + 1} and saved it to {output_pdf_path}")
