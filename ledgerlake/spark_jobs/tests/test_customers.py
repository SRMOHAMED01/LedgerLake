"""Local Spark regression tests for accepted/quarantined customer events."""

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bronze_to_silver_customers as job


def event(identifier="e1", customer_id="c1", age=30, timestamp="2026-09-01T12:00:00Z", **changes):
    value = {
        "event_id": identifier,
        "event_type": "ACCOUNT_CREATED",
        "schema_version": 1,
        "event_timestamp": timestamp,
        "producer": "customer-producer",
        "customer_id": customer_id,
        "age": age,
        "country": "MA",
        "segment": "RETAIL",
    }
    value.update(changes)
    return value


class SilverCustomerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]").appName("silver-customers-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC").getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="ledgerlake-silver-cust-")
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.bronze = root / "bronze"
        self.bronze.mkdir()
        self.silver = (root / "silver").as_posix()
        self.rejected = (root / "rejected").as_posix()

    def write(self, name, records):
        (self.bronze / name).write_text("\n".join(json.dumps(row) for row in records), encoding="utf-8")

    def rebuild(self):
        return job.rebuild_customers(
            self.spark, self.bronze.as_posix(), self.silver, self.rejected
        )

    def accepted(self):
        return self.spark.read.format("delta").load(self.silver)

    def quarantined(self):
        return self.spark.read.format("delta").load(self.rejected)

    def test_replay_and_rerun_are_idempotent_and_reconciled(self):
        self.write("events.json", [event(), event(), event("e2", age=45)])
        expected = {"input": 3, "exact_duplicates": 1, "accepted": 2, "rejected": 0}
        self.assertEqual(self.rebuild(), expected)
        self.assertEqual(self.rebuild(), expected)
        self.assertEqual(self.accepted().count(), 2)

    def test_invalid_records_are_quarantined_with_reasons(self):
        rows = [
            event("good"),
            event("bad-age", age=-5),
            event("bad-type", event_type="INVALID_TYPE"),
            event("bad-customer", customer_id=None),
            event("bad-time", event_timestamp="not-a-date"),
        ]
        self.write("events.json", rows)
        metrics = self.rebuild()
        self.assertEqual(metrics, {"input": 5, "exact_duplicates": 0, "accepted": 1, "rejected": 4})
        reasons = {row.rejection_reason for row in self.quarantined().collect()}
        self.assertEqual(reasons, {
            "INVALID_AGE", "INVALID_EVENT_TYPE", "MISSING_CUSTOMER_ID",
            "INVALID_EVENT_TIMESTAMP",
        })

    def test_conflicting_versions_are_quarantined(self):
        self.write("events.json", [event("good"), event("conflict", age=10), event("conflict", age=20)])
        metrics = self.rebuild()
        self.assertEqual(metrics["accepted"], 1)
        self.assertEqual(metrics["rejected"], 2)
        self.assertEqual(
            {row.rejection_reason for row in self.quarantined().collect()},
            {"DUPLICATE_CONFLICT"},
        )

    def test_country_segment_and_partition_transformation(self):
        self.write("events.json", [event(country="Maroc", segment="")])
        self.rebuild()
        row = self.accepted().first()
        self.assertEqual((row.country, row.country_region), ("Morocco", "Africa"))
        self.assertEqual(row.customer_segment, "UNCLASSIFIED")
        self.assertEqual((row.year, row.month, row.day), (2026, 9, 1))

    def test_all_invalid_input_preserves_previous_silver(self):
        self.write("events.json", [event()])
        self.rebuild()
        self.write("events.json", [event("bad", age=-1)])
        with self.assertRaisesRegex(ValueError, "No accepted"):
            self.rebuild()
        self.assertEqual(self.accepted().count(), 1)


if __name__ == "__main__":
    unittest.main()