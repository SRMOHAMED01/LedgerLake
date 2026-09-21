from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch


PRODUCERS = Path(__file__).resolve().parents[1] / "producers"
sys.path.insert(0, str(PRODUCERS))
import event_factory as factory


BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


class EventFactoryTests(unittest.TestCase):
    def setUp(self):
        self.customers = factory.customer_registry(42, 20)
        self.customer_ids = {item["customer_id"] for item in self.customers}

    def test_contract_files_are_valid_json_schema(self):
        from jsonschema import Draft202012Validator

        for entity in ("customer", "transaction", "loan"):
            schema = json.loads(factory.contract_path(entity).read_text(encoding="utf-8"))
            Draft202012Validator.check_schema(schema)

    def test_same_seed_produces_identical_registry_and_events(self):
        self.assertEqual(factory.customer_registry(42, 20), factory.customer_registry(42, 20))
        left = factory.transaction_event(42, 8, self.customers, BASE)
        right = factory.transaction_event(42, 8, factory.customer_registry(42, 20), BASE)
        self.assertEqual(left, right)

    def test_different_seed_changes_registry(self):
        self.assertNotEqual(self.customers, factory.customer_registry(43, 20))

    def test_customer_updates_reuse_existing_ids(self):
        created = [factory.customer_event(42, i, self.customers, BASE) for i in range(20)]
        updates = [factory.customer_event(42, i + 20, self.customers, BASE) for i in range(20)]
        self.assertEqual({event["customer_id"] for event in created},
                         {event["customer_id"] for event in updates})
        self.assertTrue(all(event["event_type"] == "ACCOUNT_CREATED" for event in created))
        self.assertTrue(all(event["event_type"] == "PROFILE_UPDATED" for event in updates))

    def test_valid_events_match_contracts_and_have_unique_event_ids(self):
        events = [factory.customer_event(42, i, self.customers, BASE) for i in range(40)]
        transactions = [factory.transaction_event(42, i, self.customers, BASE) for i in range(100)]
        loans = [event for i in range(20) for event in factory.loan_lifecycle(42, i, self.customers, BASE)]
        for event in events:
            factory.validate_event(event, "customer")
        for event in transactions:
            factory.validate_event(event, "transaction")
        for event in loans:
            factory.validate_event(event, "loan")
        identifiers = [event["event_id"] for event in events + transactions + loans]
        self.assertEqual(len(identifiers), len(set(identifiers)))

    def test_transactions_and_loans_reference_shared_customers(self):
        transactions = [factory.transaction_event(42, i, self.customers, BASE) for i in range(100)]
        loans = [event for i in range(20) for event in factory.loan_lifecycle(42, i, self.customers, BASE)]
        self.assertTrue(all(event["customer_id"] in self.customer_ids for event in transactions + loans))

    def test_loan_lifecycles_have_valid_order_and_stable_identity(self):
        for index in range(50):
            lifecycle = factory.loan_lifecycle(42, index, self.customers, BASE)
            statuses = [event["status"] for event in lifecycle]
            self.assertEqual(statuses[0], "REQUESTED")
            self.assertIn(statuses, (["REQUESTED", "REJECTED"],
                                     ["REQUESTED", "APPROVED", "REPAID"],
                                     ["REQUESTED", "APPROVED", "DEFAULTED"]))
            self.assertEqual(len({event["loan_id"] for event in lifecycle}), 1)
            times = [factory.parse_utc(event["event_timestamp"]) for event in lifecycle]
            self.assertEqual(times, sorted(times))

    def test_controlled_scenarios_are_predictable(self):
        duplicate = factory.transaction_event(42, 1, self.customers, BASE, "duplicate")
        original = factory.transaction_event(42, 0, self.customers, BASE)
        self.assertEqual(duplicate, original)
        conflict = factory.transaction_event(42, 1, self.customers, BASE, "conflicting-event")
        self.assertEqual(conflict["transaction_id"], original["transaction_id"])
        self.assertNotEqual(conflict["event_id"], original["event_id"])
        with self.assertRaises(Exception):
            factory.validate_event(factory.transaction_event(42, 1, self.customers, BASE, "invalid-amount"),
                                   "transaction")
        with self.assertRaises(Exception):
            factory.validate_event(factory.transaction_event(42, 1, self.customers, BASE, "missing-customer"),
                                   "transaction")
        late = factory.transaction_event(42, 1, self.customers, BASE, "late-event")
        self.assertLess(factory.parse_utc(late["event_timestamp"]), BASE)

    def test_contract_rejects_unknown_fields(self):
        event = factory.transaction_event(42, 1, self.customers, BASE)
        event["unexpected"] = True
        with self.assertRaises(Exception):
            factory.validate_event(event, "transaction")


class FakeProducer:
    error = None
    undelivered = 0
    produced = []
    configs = []

    def __init__(self, config):
        self.config = config
        type(self).produced = []
        type(self).configs.append(config)

    def produce(self, topic, key, value, callback):
        type(self).produced.append((topic, key, value))
        callback(type(self).error, types.SimpleNamespace())

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        return type(self).undelivered


class PublishingTests(unittest.TestCase):
    def setUp(self):
        FakeProducer.error = None
        FakeProducer.undelivered = 0
        FakeProducer.configs = []
        kafka = types.ModuleType("confluent_kafka")
        kafka.Producer = FakeProducer
        self.modules = patch.dict(sys.modules, {"confluent_kafka": kafka})
        self.modules.start()
        self.addCleanup(self.modules.stop)
        customers = factory.customer_registry(7, 2)
        self.event = factory.transaction_event(7, 0, customers, BASE)

    def test_publish_validates_serializes_and_enables_idempotence(self):
        self.assertEqual(factory.publish("transactions-events", "transaction", [self.event]), 1)
        topic, key, value = FakeProducer.produced[0]
        self.assertEqual(topic, "transactions-events")
        self.assertEqual(key.decode(), self.event["transaction_id"])
        self.assertEqual(json.loads(value), self.event)
        self.assertIs(FakeProducer.configs[0]["enable.idempotence"], True)
        self.assertEqual(FakeProducer.configs[0]["acks"], "all")

    def test_delivery_error_is_fatal(self):
        FakeProducer.error = "broker unavailable"
        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            factory.publish("transactions-events", "transaction", [self.event])

    def test_undelivered_messages_are_fatal(self):
        FakeProducer.undelivered = 1
        with self.assertRaisesRegex(RuntimeError, "undelivered=1"):
            factory.publish("transactions-events", "transaction", [self.event])


if __name__ == "__main__":
    unittest.main()
