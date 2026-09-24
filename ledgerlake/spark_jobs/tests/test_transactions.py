"""Local Spark regression tests for accepted/quarantined transactions."""

import json
from decimal import Decimal
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bronze_to_silver_transactions as job


def event(identifier="t1", amount=10.25, timestamp="2026-09-01T12:00:00Z", **changes):
    value = {
        "event_id": f"event-{identifier}",
        "event_type": "TRANSACTION_CREATED",
        "schema_version": 1,
        "event_timestamp": timestamp,
        "producer": "transaction-producer",
        "transaction_id": identifier,
        "customer_id": "c1",
        "amount": amount,
        "currency": "MAD",
        "type": "CARD_PAYMENT",
        "merchant": "Shop",
        "country": "MA",
        "timestamp": timestamp,
    }
    value.update(changes)
    return value


class SilverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]").appName("silver-transactions-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC").getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="ledgerlake-silver-")
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.bronze = root / "bronze"
        self.bronze.mkdir()
        self.silver = (root / "silver").as_posix()
        self.rejected = (root / "rejected").as_posix()

    def write(self, name, records):
        (self.bronze / name).write_text("\n".join(json.dumps(row) for row in records), encoding="utf-8")

    def rebuild(self):
        return job.rebuild_transactions(
            self.spark, self.bronze.as_posix(), self.silver, self.rejected
        )

    def accepted(self):
        return self.spark.read.parquet(self.silver)

    def quarantined(self):
        return self.spark.read.parquet(self.rejected)

    def test_replay_and_rerun_are_idempotent_and_reconciled(self):
        self.write("events.json", [event(), event(), event("t2", 20.50)])
        expected = {"input": 3, "exact_duplicates": 1, "accepted": 2, "rejected": 0}
        self.assertEqual(self.rebuild(), expected)
        self.assertEqual(self.rebuild(), expected)
        self.assertEqual(self.accepted().count(), 2)
        total = self.accepted().groupBy().sum("amount").first()[0]
        self.assertEqual(total, Decimal("30.75"))

    def test_invalid_records_are_quarantined_with_reasons(self):
        rows = [
            event("good"),
            event("bad-amount", -1),
            event("bad-currency", currency="BTC"),
            event("bad-customer", customer_id=None),
            event("bad-time", timestamp="not-a-date"),
        ]
        self.write("events.json", rows)
        metrics = self.rebuild()
        self.assertEqual(metrics, {"input": 5, "exact_duplicates": 0, "accepted": 1, "rejected": 4})
        reasons = {row.rejection_reason for row in self.quarantined().collect()}
        self.assertEqual(reasons, {
            "NON_POSITIVE_AMOUNT", "INVALID_CURRENCY", "MISSING_CUSTOMER_ID",
            "INVALID_TRANSACTION_TIMESTAMP",
        })

    def test_conflicting_versions_are_quarantined(self):
        self.write("events.json", [event("good"), event("conflict", 10), event("conflict", 20)])
        metrics = self.rebuild()
        self.assertEqual(metrics["accepted"], 1)
        self.assertEqual(metrics["rejected"], 2)
        self.assertEqual(
            {row.rejection_reason for row in self.quarantined().collect()},
            {"DUPLICATE_CONFLICT"},
        )

    def test_decimal_country_and_partition_transformation(self):
        self.write("events.json", [event()])
        self.rebuild()
        row = self.accepted().first()
        self.assertEqual(row.amount, Decimal("10.25"))
        self.assertEqual((row.country, row.country_region), ("Morocco", "Africa"))
        self.assertEqual((row.year, row.month, row.day), (2026, 9, 1))

    def test_all_invalid_input_preserves_previous_silver(self):
        self.write("events.json", [event()])
        self.rebuild()
        self.write("events.json", [event("bad", -1)])
        with self.assertRaisesRegex(ValueError, "No accepted"):
            self.rebuild()
        self.assertEqual(self.accepted().count(), 1)

    def test_missing_contract_column_fails(self):
        broken = event()
        del broken["event_id"]
        self.write("events.json", [broken])
        with self.assertRaisesRegex(ValueError, "event_id"):
            self.rebuild()

    def test_read_error_propagates(self):
        with self.assertRaises(Exception):
            job.rebuild_transactions(
                self.spark, (self.bronze / "missing").as_posix(), self.silver, self.rejected
            )


class LifecycleTests(unittest.TestCase):
    def test_session_stopped_when_rebuild_fails(self):
        spark = Mock()
        with patch.object(job, "get_spark_session", return_value=spark), patch.object(
            job, "rebuild_transactions", side_effect=OSError("offline")
        ):
            with self.assertRaises(OSError):
                job.process_transactions()
        spark.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
