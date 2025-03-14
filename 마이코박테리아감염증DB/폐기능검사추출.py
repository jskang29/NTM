import pytesseract
from pdf2image import convert_from_path

# Convert PDF to images
pdf_path = "C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\폐기능검사\\폐기능검사지.pdf"
images = convert_from_path(pdf_path)

images[0].save("C:\\Users\\내컴퓨터\\Desktop\\pdf2image\\폐기능검사지.png", "PNG")

# Perform OCR on each page
text_data = [pytesseract.image_to_string(image, lang="eng+kor") for image in images]

# Combine text from all pages
full_text = "\n".join(text_data)
full_text[:1000]  # Previewing a portion of the text to check its content

print(full_text)     