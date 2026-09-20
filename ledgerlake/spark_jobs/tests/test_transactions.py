"""Real Spark regression tests using local JSON/Parquet; no Kafka or MinIO."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bronze_to_silver_transactions as job


def event(identifier='t1', amount=10.25, timestamp='2026-09-01T12:00:00Z'):
    return dict(transaction_id=identifier, customer_id='c1', amount=amount,
                currency='MAD', type='CARD_PAYMENT', merchant='Shop', country='MA',
                timestamp=timestamp)


class SilverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[1]').appName('silver-regression')
                     .config('spark.ui.enabled', 'false')
                     .config('spark.sql.shuffle.partitions', '1')
                     .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='ledgerlake-silver-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.bronze = self.root / 'bronze'
        self.bronze.mkdir()
        self.silver = (self.root / 'silver').as_posix()

    def write(self, name, records):
        (self.bronze / name).write_text('\n'.join(json.dumps(r) for r in records), encoding='utf-8')

    def rebuild(self):
        return job.rebuild_transactions(self.spark, self.bronze.as_posix(), self.silver)

    def totals(self):
        df = self.spark.read.parquet(self.silver)
        return df.count(), df.groupBy('currency').sum('amount').collect()[0]['sum(amount)']

    def test_rerun_replay_and_late_arrival_preserve_counts_and_totals(self):
        self.write('first.json', [event(), event('t2', 20.50)])
        self.assertEqual(self.rebuild(), 2)
        self.assertEqual(self.totals(), (2, 30.75))
        self.assertEqual(self.rebuild(), 2)
        self.assertEqual(self.totals(), (2, 30.75))
        self.write('replay.json', [event(), event('t2', 20.50)])
        self.assertEqual(self.rebuild(), 2)
        self.assertEqual(self.totals(), (2, 30.75))
        self.write('late.json', [event('t3', 5.25, '2026-08-01T12:00:00Z')])
        self.assertEqual(self.rebuild(), 3)
        self.assertEqual(self.totals(), (3, 36.0))

    def test_existing_duplicates_are_repaired(self):
        self.write('first.json', [event()])
        self.rebuild()
        duplicate = job.transform_transactions(self.spark.read.json(self.bronze.as_posix()))
        duplicate.write.mode('append').partitionBy('year', 'month', 'day').parquet(self.silver)
        self.assertEqual(self.totals(), (2, 20.50))
        self.rebuild()
        self.assertEqual(self.totals(), (1, 10.25))

    def test_conflict_fails_before_replacing_previous_output(self):
        self.write('first.json', [event()])
        self.rebuild()
        self.write('conflict.json', [event(amount=999)])
        with self.assertRaisesRegex(ValueError, 'Conflicting'):
            self.rebuild()
        self.assertEqual(self.totals(), (1, 10.25))

    def test_conflict_is_not_hidden_by_invalid_amount_filter(self):
        self.write('first.json', [event(), event(amount=-1)])
        with self.assertRaisesRegex(ValueError, 'Conflicting'):
            self.rebuild()
        self.assertFalse(Path(self.silver).exists())

    def test_missing_id_rejected(self):
        self.write('first.json', [event(identifier=None)])
        with self.assertRaisesRegex(ValueError, 'transaction_id'):
            self.rebuild()

    def test_no_valid_rows_preserves_previous_output(self):
        self.write('first.json', [event()])
        self.rebuild()
        self.write('first.json', [event(amount=-1)])
        with self.assertRaisesRegex(ValueError, 'No accepted'):
            self.rebuild()
        self.assertEqual(self.totals(), (1, 10.25))

    def test_read_error_propagates(self):
        with self.assertRaises(Exception):
            job.rebuild_transactions(self.spark, (self.root / 'missing').as_posix(), self.silver)
        self.assertFalse(Path(self.silver).exists())

    def test_transformation_and_partition_date(self):
        self.write('first.json', [event(), event(), event('invalid', -2)])
        rows = job.transform_transactions(self.spark.read.json(self.bronze.as_posix())).collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0].country, rows[0].country_region), ('Morocco', 'Africa'))
        self.assertEqual((rows[0].year, rows[0].month, rows[0].day), (2026, 9, 1))


class LifecycleTests(unittest.TestCase):
    def test_session_stopped_when_rebuild_fails(self):
        spark = Mock()
        with patch.object(job, 'get_spark_session', return_value=spark), \
             patch.object(job, 'rebuild_transactions', side_effect=OSError('offline')):
            with self.assertRaises(OSError):
                job.process_transactions()
        spark.stop.assert_called_once()


if __name__ == '__main__':
    unittest.main()
