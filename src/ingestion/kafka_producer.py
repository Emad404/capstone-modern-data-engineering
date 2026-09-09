"""
Real Kafka producer for the capstone's ingestion layer (Deliverable 1).

Publishes a mix of clean and deliberately malformed order events to the
`orders_raw` topic, using the real `kafka-python` client — the same library
and pattern as the Day 2 lab's "Real Kafka Round Trip" section, applied here
to the capstone's order-event shape instead of sensor readings.

Requires a running Kafka broker at BOOTSTRAP_SERVERS. In Colab:

    !pip install kafka-python
    !curl -sSOL https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz && tar -xzf kafka_2.13-3.7.0.tgz
    !cd kafka_2.13-3.7.0 && bin/kafka-storage.sh format -t $(bin/kafka-storage.sh random-uuid) -c config/kraft/server.properties
    !cd kafka_2.13-3.7.0 && nohup bin/kafka-server-start.sh config/kraft/server.properties > /tmp/kafka.log 2>&1 &
    # wait ~15-20s for the broker to finish starting
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

TOPIC = "orders_raw"
BOOTSTRAP_SERVERS = "localhost:9092"

# 8 clean events + 3 deliberately malformed ones (bad quantity, bad country, missing id)
# so the consumer's contract gate has real failure paths to prove, not just the happy path.
SAMPLE_EVENTS = [
    {"order_id": "ORD1001", "customer_id": "CUST_88", "product_id": "SKU_001", "quantity": 2, "unit_price": 149.99, "country": "SA", "event_ts": "2026-06-29T10:00:00Z"},
    {"order_id": "ORD1002", "customer_id": "CUST_12", "product_id": "SKU_014", "quantity": 1, "unit_price": 29.50, "country": "AE", "event_ts": "2026-06-29T10:00:03Z"},
    {"order_id": "ORD1003", "customer_id": "CUST_45", "product_id": "SKU_007", "quantity": 5, "unit_price": 9.99, "country": "EG", "event_ts": "2026-06-29T10:00:07Z"},
    {"order_id": "ORD1004", "customer_id": "CUST_88", "product_id": "SKU_002", "quantity": 3, "unit_price": 59.00, "country": "SA", "event_ts": "2026-06-29T10:00:11Z"},
    {"order_id": "ORD1005", "customer_id": "CUST_23", "product_id": "SKU_009", "quantity": 1, "unit_price": 899.00, "country": "KW", "event_ts": "2026-06-29T10:00:15Z"},
    {"order_id": "ORD1006", "customer_id": "CUST_67", "product_id": "SKU_003", "quantity": 2, "unit_price": 45.25, "country": "QA", "event_ts": "2026-06-29T10:00:19Z"},
    {"order_id": "ORD1007", "customer_id": "CUST_12", "product_id": "SKU_014", "quantity": 4, "unit_price": 29.50, "country": "AE", "event_ts": "2026-06-29T10:00:23Z"},
    {"order_id": "ORD1008", "customer_id": "CUST_34", "product_id": "SKU_005", "quantity": 1, "unit_price": 199.00, "country": "BH", "event_ts": "2026-06-29T10:00:27Z"},
    # -- malformed: quantity <= 0 --
    {"order_id": "ORD1009", "customer_id": "CUST_51", "product_id": "SKU_008", "quantity": -1, "unit_price": 15.00, "country": "SA", "event_ts": "2026-06-29T10:00:31Z"},
    # -- malformed: not a real ISO country code --
    {"order_id": "ORD1010", "customer_id": "CUST_19", "product_id": "SKU_011", "quantity": 2, "unit_price": 20.00, "country": "SAUDI", "event_ts": "2026-06-29T10:00:35Z"},
    # -- malformed: missing customer_id --
    {"order_id": "ORD1011", "customer_id": "", "product_id": "SKU_006", "quantity": 1, "unit_price": 12.00, "country": "OM", "event_ts": "2026-06-29T10:00:39Z"},
]


def produce_sample_events() -> None:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for event in SAMPLE_EVENTS:
        producer.send(TOPIC, event)
        print(f"  [PRODUCER] sent -> {event['order_id']}")
    producer.flush()
    producer.close()
    print(f"\n✅ Published {len(SAMPLE_EVENTS)} events to '{TOPIC}' "
          f"({len(SAMPLE_EVENTS) - 3} clean, 3 deliberately malformed).")


if __name__ == "__main__":
    produce_sample_events()
