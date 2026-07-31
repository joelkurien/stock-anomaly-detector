import asyncio
import csv
from datetime import datetime
import io
import time
import confluent_kafka as ck
import os
import json
from aiokafka import AIOKafkaProducer
import aiofiles
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def stream_data(filepath, topic, ticker = None, delay=1):
    producer = AIOKafkaProducer(
        bootstrap_servers = os.getenv("INTERNAL_KAFKA_BOOTSTRAP_SERVERS"),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    await producer.start()
    try:
        async with aiofiles.open(filepath, 'r') as stock_file:
            content = await stock_file.read()
        
        while True:
            reader = csv.DictReader(io.StringIO(content))
            time_to_send = time.monotonic()
            for ctx in reader:
                ctx['ticker'] = ticker
                ctx['timestamp'] = datetime.now().isoformat()
                try:
                    metadata = await producer.send_and_wait(topic, key=ticker, value = ctx)
                    logger.info(f'''{ctx['timestamp']}: {ctx['ticker']} -> 
                                {metadata.topic}[{metadata.partition}@{metadata.offset}] -> 
                                {ctx['Price']}''')
                except Exception as e:
                    logger.error(f"Stock price send failed: {e}")
                time_to_send += delay
                sleep_for = time_to_send - time.monotonic()
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(stream_data("train_data1.csv", "stock-price", "JOEL"))
