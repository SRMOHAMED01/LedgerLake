"""Deterministic, related financial events and dependency-free validation."""

from __future__ import annotations

import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path


SCHEMA_VERSION = 1
NAMESPACE = uuid.UUID("43299764-733b-4ab3-81f7-e221bfb5ef19")
COUNTRIES = ("MA", "Morocco", "Maroc", "FR", "France", "US", "USA")
SEGMENTS = ("RETAIL", "PREMIUM", "BUSINESS")
CURRENCIES = ("MAD", "EUR", "USD")
TRANSACTION_TYPES = ("CARD_PAYMENT", "TRANSFER", "ATM_WITHDRAWAL", "LOAN_REPAYMENT")
LOAN_STATUSES = ("REQUESTED", "APPROVED", "REJECTED", "REPAID", "DEFAULTED")


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("BASE_EVENT_TIME must contain a timezone")
    return parsed.astimezone(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def stable_id(seed: int, *parts: object) -> str:
    return str(uuid.uuid5(NAMESPACE, ":".join(map(str, (seed, *parts)))))


def customer_registry(seed: int, size: int) -> list[dict]:
    if size < 1:
        raise ValueError("customer population must be positive")
    rng = random.Random(seed)
    return [
        {
            "customer_id": stable_id(seed, "customer", index),
            "age": rng.randint(18, 90),
            "country": rng.choice(COUNTRIES),
            "segment": rng.choice(SEGMENTS),
        }
        for index in range(size)
    ]


def metadata(seed: int, producer: str, event_type: str, index: int, occurred_at: datetime) -> dict:
    return {
        "event_id": stable_id(seed, producer, event_type, index),
        "event_type": event_type,
        "schema_version": SCHEMA_VERSION,
        "event_timestamp": iso_utc(occurred_at),
        "producer": producer,
    }


def customer_event(seed: int, index: int, customers: list[dict], base_time: datetime) -> dict:
    customer = customers[index % len(customers)]
    cycle = index // len(customers)
    event_type = "ACCOUNT_CREATED" if cycle == 0 else "PROFILE_UPDATED"
    result = metadata(seed, "customer-producer", event_type, index, base_time + timedelta(minutes=index))
    result.update(customer)
    if event_type == "PROFILE_UPDATED":
        result["segment"] = SEGMENTS[(SEGMENTS.index(customer["segment"]) + cycle) % len(SEGMENTS)]
    return result


def transaction_event(
    seed: int,
    index: int,
    customers: list[dict],
    base_time: datetime,
    scenario: str = "valid",
) -> dict:
    rng = random.Random(seed * 1_000_003 + index)
    customer = customers[index % len(customers)]
    event = metadata(seed, "transaction-producer", "TRANSACTION_CREATED", index,
                     base_time + timedelta(seconds=index))
    event.update({
        "transaction_id": stable_id(seed, "transaction", index),
        "customer_id": customer["customer_id"],
        "amount": rng.randint(100, 500_000) / 100,
        "currency": rng.choice(CURRENCIES),
        "type": rng.choice(TRANSACTION_TYPES),
        "merchant": f"Merchant-{rng.randint(1, 100):03d}",
        "country": customer["country"],
        # Kept during migration for the existing Silver job.
        "timestamp": event["event_timestamp"],
    })
    if scenario == "invalid-amount":
        event["amount"] = -event["amount"]
    elif scenario == "missing-customer":
        event["customer_id"] = None
    elif scenario == "late-event":
        late = base_time - timedelta(days=7, seconds=index)
        event["event_timestamp"] = event["timestamp"] = iso_utc(late)
    elif scenario not in ("valid", "duplicate", "conflicting-event"):
        raise ValueError(f"Unknown scenario: {scenario}")
    if scenario == "duplicate" and index > 0:
        return transaction_event(seed, index - 1, customers, base_time)
    if scenario == "conflicting-event" and index > 0:
        event["transaction_id"] = stable_id(seed, "transaction", index - 1)
    return event


def loan_lifecycle(seed: int, index: int, customers: list[dict], base_time: datetime) -> list[dict]:
    rng = random.Random(seed * 2_000_003 + index)
    customer = customers[index % len(customers)]
    loan_id = stable_id(seed, "loan", index)
    amount = rng.randint(500_000, 50_000_000) / 100
    duration = rng.choice((12, 24, 36, 48, 60, 120, 240, 300))
    terminal = rng.choice(("REJECTED", "REPAID", "DEFAULTED"))
    statuses = ["REQUESTED", terminal] if terminal == "REJECTED" else ["REQUESTED", "APPROVED", terminal]
    events = []
    for step, status in enumerate(statuses):
        event_index = index * 10 + step
        event = metadata(seed, "loan-producer", f"LOAN_{status}", event_index,
                         base_time + timedelta(days=step, seconds=index))
        event.update({
            "loan_id": loan_id,
            "customer_id": customer["customer_id"],
            "amount": amount,
            "duration_months": duration,
            "status": status,
            "timestamp": event["event_timestamp"],
        })
        events.append(event)
    return events


def validate_event(event: dict, entity: str) -> None:
    """Validate against the versioned contract plus cross-field rules."""
    from jsonschema import Draft202012Validator, FormatChecker

    schema = json.loads(contract_path(entity).read_text(encoding="utf-8"))
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(event)
    if entity in ("transaction", "loan") and Decimal(str(event["amount"])).as_tuple().exponent < -2:
        raise ValueError("Monetary amount cannot contain more than two decimal places")
    if entity == "loan" and event["event_type"] != f"LOAN_{event['status']}":
        raise ValueError("Loan event_type and status disagree")


def producer_settings(description: str) -> argparse.Namespace:
    from dotenv import load_dotenv

    load_dotenv()
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--count", type=int, default=int(os.getenv("EVENT_COUNT", "0")),
                        help="0 streams until interrupted")
    parser.add_argument("--seed", type=int, default=int(os.getenv("EVENT_SEED", "20260921")))
    parser.add_argument("--customers", type=int, default=int(os.getenv("CUSTOMER_POPULATION", "1000")))
    parser.add_argument("--interval", type=float, default=float(os.getenv("EVENT_INTERVAL_SECONDS", "0")))
    parser.add_argument("--base-time", default=os.getenv("BASE_EVENT_TIME", "2026-01-01T00:00:00Z"))
    parser.add_argument("--scenario", default=os.getenv("EVENT_SCENARIO", "valid"))
    return parser.parse_args()


def publish(topic: str, entity: str, events, interval: float = 0, validate: bool = True) -> int:
    """Validate and publish; return only after all deliveries are confirmed."""
    from confluent_kafka import Producer
    import time

    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    failures = []

    def delivered(error, message):
        if error is not None:
            failures.append(str(error))

    producer = Producer({"bootstrap.servers": broker, "enable.idempotence": True, "acks": "all"})
    count = 0
    for event in events:
        if validate:
            validate_event(event, entity)
        key = event.get(f"{entity}_id", event["event_id"])
        producer.produce(topic, key=key.encode(), value=json.dumps(event, separators=(",", ":")).encode(),
                         callback=delivered)
        producer.poll(0)
        count += 1
        if interval:
            time.sleep(interval)
    undelivered = producer.flush(30)
    if failures or undelivered:
        raise RuntimeError(f"Kafka delivery failed: errors={failures}, undelivered={undelivered}")
    return count


def contract_path(entity: str) -> Path:
    return Path(__file__).resolve().parents[1] / "contracts" / f"{entity}.v1.schema.json"
