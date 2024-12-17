# Gunakan base image Python
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Salin file requirements dan kode
COPY requirements.txt ./
COPY . .

# Install dependencies
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Default command
CMD ["python", "your_script.py"]
