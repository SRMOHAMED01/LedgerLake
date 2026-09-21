"""Publish deterministic transactions linked to the shared customer registry."""

import itertools
import os

from event_factory import customer_registry, parse_utc, producer_settings, publish, transaction_event


def events(settings):
    customers = customer_registry(settings.seed, settings.customers)
    indexes = range(settings.count) if settings.count else itertools.count()
    base_time = parse_utc(settings.base_time)
    for index in indexes:
        yield transaction_event(settings.seed, index, customers, base_time, settings.scenario)


def main():
    settings = producer_settings("Publish related LedgerLake transaction events")
    topic = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions-events")
    intentional_invalid = settings.scenario in ("invalid-amount", "missing-customer")
    print(f"Published {publish(topic, 'transaction', events(settings), settings.interval,
                               validate=not intentional_invalid)} transaction events")


if __name__ == "__main__":
    main()
