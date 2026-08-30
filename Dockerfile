nnFROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulate_producer.py .
COPY simulate_consumer.py .
COPY asset_ticker.txt .
COPY train_data1.csv .

CMD ["python", "simulate_producer.py"]
