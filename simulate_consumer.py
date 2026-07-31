import json
import os 
from dotenv import load_dotenv
from aiokafka import AIOKafkaConsumer
import logging
import asyncio

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def consume_data(topic, ticker=None, delay=1):
    logger.info(os.getenv("EXTERNAL_KAFKA_BOOTSTRAP_SERVERS"))
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers = os.getenv("EXTERNAL_KAFKA_BOOTSTRAP_SERVERS"),
        value_deserializer = lambda v: json.loads(v.decode('utf-8')),
        key_deserializer = lambda k: k.decode('utf-8') if k else None
    )
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                if msg is None:
                    raise ValueError("The received message is empty")
                logger.info(f'''{msg.topic}[{msg.partition}@{msg.offset}] ->
                            {msg.key}: {msg.value}''')
            except Exception as e:
                logger.error(f"Failed to consume stock price: {e}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume_data("stock-price", "JOEL"))