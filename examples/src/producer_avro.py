# pip install aiokafka -U

import aiokafka
import asyncio


async def produce_message(topic: str, message: str):
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers="localhost:9092")
    await producer.start()
    try:
        await producer.send_and_wait(topic, message)
    finally:
        await producer.stop()


async def main():
    # Schedule three calls *concurrently*:
    with open('avro-stream-test.avro', 'rb') as content_file:
        data = content_file.read()
        for i in range(0, 10_000):
            await produce_message("avro", data)
            await produce_message("avrotyped", data)
            print(f"Produced {i}")
        print("DONE")


if __name__ == "__main__":
    asyncio.run(main())
