FROM python:3.8

# Install Sawtooth SDK dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libzmq3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy transaction processor code
COPY federated_schedule_tp.py .

# Set environment variables
ENV PYTHONPATH=/app

# Run the transaction processor
CMD ["python", "federated_schedule_tp.py"]