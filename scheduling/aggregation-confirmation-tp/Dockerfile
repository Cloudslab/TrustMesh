FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY aggregation_confirmation_tp.py .

CMD ["python", "aggregation_confirmation_tp.py"]