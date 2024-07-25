import fitz  # PyMuPDF
import tkinter as tk
from tkinter import filedialog, simpledialog, messagebox
from PIL import Image, ImageTk

class PDFEditor:
    def __init__(self, root):
        self.root = root
        self.root.title("PDF Editor")
        self.input_pdf_path = ""
        self.output_pdf_path = ""
        self.current_page_image = None
        self.current_page = 0
        self.doc = None

        self.canvas = tk.Canvas(root, width=800, height=1000)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.create_widgets()

    def create_widgets(self):
        btn_frame = tk.Frame(self.root)
        btn_frame.pack(side=tk.RIGHT, fill=tk.Y)

        tk.Button(btn_frame, text="Open PDF", command=self.open_pdf).pack(padx=5, pady=5, fill=tk.X)
        tk.Button(btn_frame, text="Next Page", command=self.next_page).pack(padx=5, pady=5, fill=tk.X)
        tk.Button(btn_frame, text="Previous Page", command=self.previous_page).pack(padx=5, pady=5, fill=tk.X)
        tk.Button(btn_frame, text="Add Text", command=self.add_text_dialog).pack(padx=5, pady=5, fill=tk.X)
        tk.Button(btn_frame, text="Save PDF", command=self.save_pdf).pack(padx=5, pady=5, fill=tk.X)

    def open_pdf(self):
        self.input_pdf_path = filedialog.askopenfilename(filetypes=[("PDF files", "*.pdf")])
        if self.input_pdf_path:
            self.doc = fitz.open(self.input_pdf_path)
            self.current_page = 0
            self.display_page(self.current_page)

    def display_page(self, page_number):
        if self.doc is None:
            return
        page = self.doc.load_page(page_number)
        pix = page.get_pixmap()
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        self.current_page_image = ImageTk.PhotoImage(img)
        self.canvas.create_image(0, 0, anchor=tk.NW, image=self.current_page_image)
        self.canvas.config(scrollregion=self.canvas.bbox(tk.ALL))

    def next_page(self):
        if self.doc and self.current_page < len(self.doc) - 1:
            self.current_page += 1
            self.display_page(self.current_page)

    def previous_page(self):
        if self.doc and self.current_page > 0:
            self.current_page -= 1
            self.display_page(self.current_page)

    def add_text_dialog(self):
        if self.doc is None:
            messagebox.showerror("Error", "Please open a PDF file first")
            return

        text = simpledialog.askstring("Input", "Enter the text to add:")
        x = simpledialog.askinteger("Input", "Enter the x coordinate:")
        y = simpledialog.askinteger("Input", "Enter the y coordinate:")

        if text and x is not None and y is not None:
            self.add_text(text, x, y, self.current_page)
            self.display_page(self.current_page)

    def add_text(self, text, x, y, page_number):
        page = self.doc.load_page(page_number)
        page.insert_text((x, y), text, fontsize=12, color=(0, 0, 0))

    def save_pdf(self):
        if self.doc is None:
            messagebox.showerror("Error", "No PDF opened")
            return

        self.output_pdf_path = filedialog.asksaveasfilename(defaultextension=".pdf", filetypes=[("PDF files", "*.pdf")])
        if self.output_pdf_path:
            self.doc.save(self.output_pdf_path)
            messagebox.showinfo("Success", f"PDF saved to {self.output_pdf_path}")

def main():
    root = tk.Tk()
    app = PDFEditor(root)
    root.mainloop()

if __name__ == "__main__":
    main()
