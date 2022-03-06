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
    # Schedule 10 calls *concurrently*:
    l = [produce_message("example", f"{i * (1_000_000 + i)}") for i in range(0, 1_000)]
    L = await asyncio.gather(*l)
    print("DONE")
    # print(L)


if __name__ == "__main__":
    asyncio.run(main())
