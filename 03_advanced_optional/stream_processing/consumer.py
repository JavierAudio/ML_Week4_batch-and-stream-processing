from kafka import KafkaConsumer, KafkaProducer
from kafka.serializer import JsonSerializer
from settings import (
    BROKER_URL,
    HIGH_AMOUNT_TOPIC,
    LOW_AMOUNT_TOPIC,
    SOURCE_TOPIC,
)

consumer = KafkaConsumer(
    SOURCE_TOPIC,
    bootstrap_servers=[BROKER_URL],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="mle-redpanda-demo",
    value_deserializer=JsonSerializer(),
)

producer = KafkaProducer(
    bootstrap_servers=[BROKER_URL],
    value_serializer=JsonSerializer(),
)


def main():
    print("Waiting for rides...")
    for message in consumer:
        event = message.value
        topic = HIGH_AMOUNT_TOPIC if event["total_amount"] >= 25.0 else LOW_AMOUNT_TOPIC
        producer.send(topic, value=event)
        producer.flush()
        print(f"Forwarded ride with total_amount={event['total_amount']} to {topic}")


if __name__ == "__main__":
    main()
