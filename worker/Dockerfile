
FROM python:3.12-slim

WORKDIR /app

COPY requirements-storage.txt .

RUN pip install --no-cache-dir -r requirements-storage.txt

COPY worker/worker.py .

CMD ["python", "-u", "worker.py"]
