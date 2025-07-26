FROM python:3.8-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -f https://download.pytorch.org/whl/torch_stable.html

# Copy application code
COPY validation_dataset_distributor.py .

# Make the script executable
RUN chmod +x validation_dataset_distributor.py

# Set the entry point
CMD ["python", "validation_dataset_distributor.py"]