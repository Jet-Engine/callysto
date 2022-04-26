# pip install aiokafka -U

import aiokafka
import asyncio


async def produce_message(topic: str, message: str):
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers="localhost:9092")
    await producer.start()
    try:
        await producer.send_and_wait(topic, message.encode())
    finally:
        await producer.stop()


async def main():
    data = "hello"
    for i in range(0, 100):
        await produce_message("example", data)
        print("Produced on topic `example`")
    print("DONE")


if __name__ == "__main__":
    asyncio.run(main())
