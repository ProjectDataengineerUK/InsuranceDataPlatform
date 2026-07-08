FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/__init__.py src/__init__.py
COPY src/ingestion/ src/ingestion/

ENV PYTHONPATH=/app

CMD ["python", "-m", "src.ingestion.producer.main"]
