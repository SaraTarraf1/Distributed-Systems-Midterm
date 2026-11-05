FROM python:3.12-slim

WORKDIR /app

COPY requirements-storage.txt .
RUN pip install --no-cache-dir -r requirements-storage.txt

COPY gateway/app.py app.py
COPY gateway/chunking.py chunking.py
COPY gateway/elasticsearch_index.py elasticsearch_index.py
COPY gateway/monitoring.py monitoring.py

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]