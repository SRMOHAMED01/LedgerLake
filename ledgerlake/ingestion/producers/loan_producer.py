"""Publish deterministic, ordered loan lifecycles linked to customers."""

import itertools
import os

from event_factory import customer_registry, loan_lifecycle, parse_utc, producer_settings, publish


def events(settings):
    if settings.scenario != "valid":
        raise ValueError("Loan producer currently supports only the valid scenario")
    customers = customer_registry(settings.seed, settings.customers)
    indexes = range(settings.count) if settings.count else itertools.count()
    base_time = parse_utc(settings.base_time)
    for index in indexes:
        yield from loan_lifecycle(settings.seed, index, customers, base_time)


def run_batch(count: int = 1000, seed: int = 20260921, customers: int = 1000,
              base_time: str = "2026-01-01T00:00:00Z", scenario: str = "valid",
              interval: float = 0.0) -> int:
    """Run producer in batch mode, publishing `count` events (each loan = 2-3 events)."""
    class Settings:
        pass

    settings = Settings()
    settings.count = count
    settings.seed = seed
    settings.customers = customers
    settings.base_time = base_time
    settings.scenario = scenario
    settings.interval = interval

    if scenario != "valid":
        raise ValueError("Loan producer currently supports only the valid scenario")
    topic = os.getenv("KAFKA_TOPIC_LOANS", "loan-events")
    return publish(topic, 'loan', events(settings), interval)


def main():
    settings = producer_settings("Publish related LedgerLake loan lifecycles")
    topic = os.getenv("KAFKA_TOPIC_LOANS", "loan-events")
    print(f"Published {publish(topic, 'loan', events(settings), settings.interval)} loan events")


if __name__ == "__main__":
    main()
