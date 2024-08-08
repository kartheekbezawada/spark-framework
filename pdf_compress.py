import fitz  # PyMuPDF
from PIL import Image
import io
import os

def compress_image(image_bytes, image_ext, quality):
    image = Image.open(io.BytesIO(image_bytes))
    compressed_image_io = io.BytesIO()
    image.save(compressed_image_io, format=image_ext, quality=quality)
    return compressed_image_io.getvalue()

def compress_pdf(input_pdf_path, output_pdf_path, image_quality):
    pdf_document = fitz.open(input_pdf_path)
    pdf_writer = fitz.open()  # Create a new PDF for output

    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        image_list = page.get_images(full=True)
        
        for img_index, img in enumerate(image_list):
            xref = img[0]
            base_image = pdf_document.extract_image(xref)
            image_bytes = base_image["image"]
            image_ext = base_image["ext"]
            
            # Compress the image
            compressed_image_bytes = compress_image(image_bytes, image_ext, image_quality)
            
            # Create a temporary file to replace the image in PDF
            temp_img_path = f"temp_image_{page_num}_{img_index}.{image_ext}"
            with open(temp_img_path, "wb") as f:
                f.write(compressed_image_bytes)
            
            # Replace the image in PDF
            page.replace_image(xref, fitz.open(temp_img_path))

            # Clean up the temporary file
            os.remove(temp_img_path)
        
        pdf_writer.insert_pdf(pdf_document, from_page=page_num, to_page=page_num)

    pdf_writer.save(output_pdf_path)
    pdf_writer.close()
    pdf_document.close()

# Define the input PDF file path
input_pdf_path = r'C:/Users/Aryan/OneDrive/Anupama ILR/Combining/merged_all_pages.pdf'

# Define the output PDF file path
output_pdf_path = r'C:/Users/Aryan/OneDrive/Anupama ILR/Combining/merged_all_pages2.pdf'

# Define the image quality for compression (0-100)
image_quality = 50

# Call the function to compress the PDF file
