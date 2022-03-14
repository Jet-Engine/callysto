# pip install aiokafka -U

import aiokafka
import asyncio


async def produce_message(topic: str, message: str):
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers="localhost:9092")
    await producer.start()
    try:
        for x in range(0, 10_000):
            await producer.send_and_wait(topic, message.encode())
    finally:
        await producer.stop()


async def main():
    # Schedule three calls *concurrently*:
    data = (
        "https://open.spotify.com/track/5qdhrNibheeUS7HVSJ1m3T?si=0fb129ddc04e489c"
        * 1000
    )
    L = await asyncio.gather(
        produce_message("double-agent-1", data), produce_message("double-agent-2", data)
    )
    print(L)


if __name__ == "__main__":
    # asyncio.run(produce_message("example", "https://open.spotify.com/track/5qdhrNibheeUS7HVSJ1m3T?si=0fb129ddc04e489c"))
    # asyncio.run(produce_message("double-agent-1", "https://open.spotify.com/track/5qdhrNibheeUS7HVSJ1m3T?si=0fb129ddc04e489c"))
    # asyncio.run(produce_message("double-agent-2", "https://open.spotify.com/track/5qdhrNibheeUS7HVSJ1m3T?si=0fb129ddc04e489c"))
    asyncio.run(main())
