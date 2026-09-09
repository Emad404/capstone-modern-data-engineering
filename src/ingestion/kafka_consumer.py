"""
Real Kafka consumer for the capstone's ingestion layer (Deliverable 1).

Reads every event published to `orders_raw`, runs it through the
`OrderEventContract` gate in contracts.py, and splits the batch:
  - valid events  -> returned for the lakehouse Bronze writer to pick up
  - invalid events -> written to data/quarantine/orders.jsonl as
    QuarantineRecord rows (raw payload + rejection reason), never silently
    dropped.

Same library and topic-consumption pattern as the Day 2 lab's real Kafka
round trip; the schema-gate logic itself is the OrderEventContract shared
with the rest of the capstone (see contracts.py) instead of an inline check.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from src.ingestion.contracts import validate_event

TOPIC = "orders_raw"
BOOTSTRAP_SERVERS = "localhost:9092"
QUARANTINE_PATH = Path("data/quarantine/orders.jsonl")


def consume_and_gate(max_messages: int = 11, timeout_s: int = 15) -> tuple[list[dict], list[dict]]:
    """Consume up to `max_messages` from Kafka, gate each through the contract.

    Returns (valid_events, quarantined_records).
    """
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        consumer_timeout_ms=timeout_s * 1000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    valid, quarantined = [], []
    for message in consumer:
        raw = message.value
        event, quarantine_record = validate_event(raw, source_topic=TOPIC)
        if event is not None:
            valid.append(event.model_dump())
            print(f"  [CONSUMER] ✅ accepted {event.order_id}")
        else:
            quarantined.append(quarantine_record.model_dump())
            print(f"  [CONSUMER] ❌ quarantined {raw.get('order_id', '?')} -> {quarantine_record.reason}")
    consumer.close()

    _write_quarantine(quarantined)
    print(f"\n✅ Ingestion gate result: {len(valid)} accepted, {len(quarantined)} quarantined.")
    return valid, quarantined


def _write_quarantine(records: list[dict]) -> None:
    QUARANTINE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(QUARANTINE_PATH, "a") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


if __name__ == "__main__":
    consume_and_gate()
