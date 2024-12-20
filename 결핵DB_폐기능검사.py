from PyPDF2 import PdfReader
from PIL import Image
from pdf2image import convert_from_path
import pytesseract
import re

# Load the uploaded PDF
pdf_path = 'C:\\Users\\JBCP_01\\Desktop\\감염증_구축\\폐기능검사\\00596102_DLCO.pdf'
reader = PdfReader(pdf_path)

# Extract text from each page
pdf_text = [page.extract_text() for page in reader.pages]
pdf_text

# Convert PDF to images for OCR processing
images = convert_from_path(pdf_path)

# Perform OCR on each image and collect text
ocr_text = [pytesseract.image_to_string(image, lang='kor+eng') for image in images]
ocr_text

# Combine OCR text for processing
combined_text = " ".join(ocr_text)

# Extract relevant values using regex
height_match = re.search(r"Height\(cm\):\s*(\d+)", combined_text)
weight_match = re.search(r"Weight\(kg\):\s*([\d.]+)", combined_text)
fev1_pre_match = re.search(r"FEV1\s+Liters\s+\d+\.\d+\s+(\d+)\s", combined_text)
fvc_pre_match = re.search(r"FVC\s+Liters\s+\d+\.\d+\s+(\d+)\s", combined_text)
fev1_fvc_pre_match = re.search(r"FEV1/FVC\s+\d+%\s+(\d+)", combined_text)
dlco_pre_match = re.search(r"DLCO\s+mL/mmHg/min\s+\d+\.\d+\s+(\d+)", combined_text)

# Store the extracted values
extracted_data = {
    "Height (cm)": height_match.group(1) if height_match else None,
    "Weight (kg)": weight_match.group(1) if weight_match else None,
    "FEV1 (%)": fev1_pre_match.group(1) if fev1_pre_match else None,
    "FVC (%)": fvc_pre_match.group(1) if fvc_pre_match else None,
    "FEV1/FVC (%)": fev1_fvc_pre_match.group(1) if fev1_fvc_pre_match else None,
    "DLCO (%)": dlco_pre_match.group(1) if dlco_pre_match else None,
}

extracted_data
