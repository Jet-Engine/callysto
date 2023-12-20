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
    # Schedule three calls *concurrently*:
    data = (
        "https://open.spotify.com/track/5qdhrNibheeUS7HVSJ1m3T?si=0fb129ddc04e489c"
    )
    for i in range(0, 10_000):
        await produce_message("elastic-input", data)
        print(f"Produced {i}")
    print("DONE")


if __name__ == "__main__":
    # asyncio.run(produce_message("example", "https://open.spotify.com/track/5qdhrNibheeUS7HVSJ1m3T?si=0fb129ddc04e489c"))
    # asyncio.run(produce_message("double-agent-1", "https://open.spotify.com/track/5qdhrNibheeUS7HVSJ1m3T?si=0fb129ddc04e489c"))
    # asyncio.run(produce_message("double-agent-2", "https://open.spotify.com/track/5qdhrNibheeUS7HVSJ1m3T?si=0fb129ddc04e489c"))
    asyncio.run(main())
