"""
Data contract enforced at the ingestion boundary (Deliverable 1 — Ingestion, 20pt).

Every record entering the pipeline through Kafka must pass this Pydantic v2 model
before it is allowed anywhere near the Bronze layer. Anything that fails validation
is routed to the quarantine dead-letter store with the rejection reason attached —
it is never silently dropped and never allowed to corrupt Bronze.

This mirrors the RetailTransactionContract pattern from Day 4's lab and the
BookOrderContract pattern from the Day 4 exercise, applied here to the capstone's
own event shape (e-commerce order events, the same domain used across Day 1 and
Day 4's material).
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator

ISO_COUNTRY_RE = re.compile(r"^[A-Z]{2}$")


class OrderEventContract(BaseModel):
    """
    Machine-enforceable schema + business rules for one order event.

    strict=False: incoming Kafka payloads are JSON, so numeric-looking strings
    ("12.50") are coerced — this matches the "strict for internal contracts,
    lenient at the wire boundary" pattern used in the Day 4 lab. The *business*
    rules below (non-empty IDs, positive quantity, valid country) are what
    actually gates the record, not the coercion behaviour.
    """

    model_config = ConfigDict(strict=False, extra="forbid")

    order_id: str
    customer_id: str
    product_id: str
    quantity: int
    unit_price: float
    country: str
    event_ts: str

    @field_validator("order_id", "customer_id", "product_id")
    @classmethod
    def not_blank(cls, v: str) -> str:
        if not v or not str(v).strip():
            raise ValueError("required identifier field is missing or blank")
        return v

    @field_validator("quantity")
    @classmethod
    def positive_quantity(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f"quantity must be > 0, got {v}")
        return v

    @field_validator("unit_price")
    @classmethod
    def positive_price(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"unit_price must be > 0, got {v}")
        return v

    @field_validator("country")
    @classmethod
    def valid_country_code(cls, v: str) -> str:
        if not ISO_COUNTRY_RE.match(v or ""):
            raise ValueError(f"country must be a 2-letter ISO code, got {v!r}")
        return v

    @field_validator("event_ts")
    @classmethod
    def parseable_timestamp(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"event_ts is not a valid ISO-8601 timestamp: {v!r}") from exc
        return v


class QuarantineRecord(BaseModel):
    """What actually lands in the dead-letter store — the raw payload plus why it failed."""

    raw_payload: dict
    reason: str
    rejected_at: str
    source_topic: str


def validate_event(raw: dict, source_topic: str) -> tuple[Optional[OrderEventContract], Optional[QuarantineRecord]]:
    """Validate one raw event against the contract.

    Returns (validated_event, None) on success, or (None, quarantine_record) on failure.
    This is the single choke point every ingestion path (real Kafka consumer, and the
    batch replay path used when no broker is available) calls through, so the
    accept/reject behaviour is identical either way.
    """
    try:
        return OrderEventContract(**raw), None
    except Exception as exc:  # noqa: BLE001
        return None, QuarantineRecord(
            raw_payload=raw,
            reason=str(exc),
            rejected_at=datetime.utcnow().isoformat() + "Z",
            source_topic=source_topic,
        )
