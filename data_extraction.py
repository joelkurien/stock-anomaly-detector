import websockets
import asyncio
from dotenv import load_dotenv
import confluent_kafka as ck
import os
import json

load_dotenv()

FINNHUB_KEY = os.getenv("FINNHUB_API_KEY")

kafka_producer = ck.Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    })

async def handle_message(msg):
    data = json.loads(msg)
    if 'type' in data.keys() and data["type"] == 'trade':
        for tick in data["data"]:
            symbol = tick.get('s')
            kafka_producer.produce(
                topic="crypto",
                key=symbol,
                value = json.dumps(tick)
            )
            print(tick)

async def connect(ticker_list):
    url = f"wss://ws.finnhub.io?token={FINNHUB_KEY}"

    async with websockets.connect(url) as ws:
        for ticker in ticker_list:
            await ws.send(json.dumps({
                "type": "subscribe",
                "symbol": ticker
            }))
        while True:
            await handle_message(await ws.recv())

ticker_list = []
with open("asset_ticker.txt", "r") as asset:
    contents = asset.readlines()
    for content in contents:
        ticker_data = content.split(",")
        ticker_list.append(ticker_data[1].strip())

asyncio.run(connect(ticker_list))
                
