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

# Pre-download MNIST dataset to avoid runtime network issues
RUN python -c "
import os
from torchvision import datasets, transforms
print('Pre-downloading MNIST dataset for validation distributor...')
os.makedirs('/tmp/mnist', exist_ok=True)
transform = transforms.Compose([transforms.ToTensor()])
# Download both training and test datasets
train_dataset = datasets.MNIST(root='/tmp/mnist', train=True, download=True, transform=transform)
test_dataset = datasets.MNIST(root='/tmp/mnist', train=False, download=True, transform=transform)
print(f'MNIST dataset downloaded: {len(train_dataset)} train samples, {len(test_dataset)} test samples')
"

# Copy application code
COPY validation_dataset_distributor.py .

# Make the script executable
RUN chmod +x validation_dataset_distributor.py

# Set the entry point
CMD ["python", "validation_dataset_distributor.py"]