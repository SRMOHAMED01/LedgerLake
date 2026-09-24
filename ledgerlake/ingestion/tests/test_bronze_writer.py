"""Offline fault-injection tests; no broker, credentials or third-party packages needed."""
import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch


class TopicPartition:
    def __init__(self, topic, partition, offset=None, error=None):
        self.topic, self.partition, self.offset, self.error = topic, partition, offset, error


class ClientError(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


def load_writer():
    kafka = types.ModuleType('confluent_kafka')
    kafka.Consumer = Mock()
    kafka.TopicPartition = TopicPartition
    kafka.KafkaException = RuntimeError
    kafka.KafkaError = types.SimpleNamespace(_PARTITION_EOF=-191)
    boto = types.ModuleType('boto3')
    boto.client = Mock()
    dotenv = types.ModuleType('dotenv')
    dotenv.load_dotenv = Mock()
    errors = types.ModuleType('botocore.exceptions')
    errors.ClientError = ClientError
    modules = {'confluent_kafka': kafka, 'boto3': boto, 'dotenv': dotenv,
               'botocore': types.ModuleType('botocore'), 'botocore.exceptions': errors}
    path = Path(__file__).resolve().parents[1] / 'bronze_writer.py'
    spec = importlib.util.spec_from_file_location('writer_under_test', path)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, modules):
        spec.loader.exec_module(module)
    return module


writer_module = load_writer()


def message(offset=0, partition=0, topic='transactions-events', payload=b'{"amount":10}'):
    result = Mock()
    result.topic.return_value = topic
    result.partition.return_value = partition
    result.offset.return_value = offset
    result.value.return_value = payload
    result.error.return_value = None
    return result


class WriterTests(unittest.TestCase):
    def setUp(self):
        self.consumer = Mock()
        self.consumer.commit.return_value = []
        self.s3 = Mock()
        self.writer = writer_module.BronzeWriter(self.consumer, self.s3)

    def test_write_precedes_scoped_commit_and_preserves_payload(self):
        order = []
        self.s3.put_object.side_effect = lambda **kw: order.append('write')
        self.consumer.commit.side_effect = lambda **kw: order.append('commit') or []
        key = self.writer.add(message(offset=8, payload=b'{ "amount": 10 }'))
        self.writer.add(message(offset=9))
        other = self.writer.add(message(offset=30, partition=1))
        self.writer.flush(key)
        self.assertEqual(order, ['write', 'commit'])
        committed = self.consumer.commit.call_args.kwargs['offsets'][0]
        self.assertEqual((committed.topic, committed.partition, committed.offset),
                         ('transactions-events', 0, 10))
        self.assertFalse(self.consumer.commit.call_args.kwargs['asynchronous'])
        self.assertEqual(self.s3.put_object.call_args.kwargs['Body'],
                         b'{ "amount": 10 }\n{"amount":10}')
        self.assertNotIn(key, self.writer.batches)
        self.assertIn(other, self.writer.batches)

    def test_storage_failure_retains_batch_and_never_commits(self):
        key = self.writer.add(message())
        self.s3.put_object.side_effect = OSError('storage offline')
        with self.assertRaises(OSError):
            self.writer.flush(key)
        self.consumer.commit.assert_not_called()
        self.assertEqual(len(self.writer.batches[key]), 1)

    def test_commit_exception_retains_batch(self):
        key = self.writer.add(message())
        self.consumer.commit.side_effect = RuntimeError('broker offline')
        with self.assertRaises(RuntimeError):
            self.writer.flush(key)
        self.assertIn(key, self.writer.batches)

    def test_partition_commit_error_retains_batch(self):
        key = self.writer.add(message())
        self.consumer.commit.return_value = [TopicPartition(key[0], key[1], error='denied')]
        with self.assertRaises(RuntimeError):
            self.writer.flush(key)
        self.assertIn(key, self.writer.batches)

    def test_failure_stops_processing_and_closes_without_final_flush(self):
        self.consumer.poll.side_effect = [message(), message(offset=1)]
        self.s3.put_object.side_effect = OSError('offline')
        with self.assertRaises(OSError):
            self.writer.run(batch_size=1)
        self.assertEqual(self.consumer.poll.call_count, 1)
        self.s3.put_object.assert_called_once()
        self.consumer.commit.assert_not_called()
        self.consumer.close.assert_called_once()

    def test_restart_replays_uncommitted_event(self):
        event = message(offset=5)
        key = self.writer.add(event)
        self.s3.put_object.side_effect = OSError('offline')
        with self.assertRaises(OSError):
            self.writer.flush(key)
        # Simulate Kafka redelivery from the unchanged committed position.
        restarted = writer_module.BronzeWriter(self.consumer, Mock())
        restarted.add(event)
        restarted.flush(key)
        self.assertEqual(self.consumer.commit.call_args.kwargs['offsets'][0].offset, 6)

    def test_shutdown_flushes_and_closes(self):
        self.consumer.poll.side_effect = [message(), KeyboardInterrupt()]
        self.writer.run()
        self.s3.put_object.assert_called_once()
        self.consumer.commit.assert_called_once()
        self.consumer.close.assert_called_once()

    def test_shutdown_failure_still_closes_without_commit(self):
        self.consumer.poll.side_effect = [message(), KeyboardInterrupt()]
        self.s3.put_object.side_effect = OSError('offline')
        with self.assertRaises(OSError):
            self.writer.run()
        self.consumer.close.assert_called_once()
        self.consumer.commit.assert_not_called()

    def test_timeout_flushes_small_batch(self):
        self.consumer.poll.side_effect = [message(), None, KeyboardInterrupt()]
        with patch.object(writer_module.time, 'monotonic', side_effect=[0, 1, 11]):
            self.writer.run()
        self.s3.put_object.assert_called_once()
        self.consumer.commit.assert_called_once()

    def test_rebalance_discards_only_released_buffers_without_commit(self):
        key = self.writer.add(message())
        other = self.writer.add(message(partition=1))
        self.writer.release_partitions(self.consumer, [TopicPartition(*key)])
        self.assertNotIn(key, self.writer.batches)
        self.assertIn(other, self.writer.batches)
        self.consumer.commit.assert_not_called()
        self.s3.put_object.assert_not_called()

    def test_tombstone_is_not_acknowledged(self):
        with self.assertRaises(ValueError):
            self.writer.add(message(payload=None))
        self.consumer.commit.assert_not_called()

    def test_bucket_permission_error_is_fatal(self):
        self.s3.head_bucket.side_effect = ClientError('403')
        with self.assertRaises(ClientError):
            writer_module.create_bucket_if_not_exists(self.s3, 'bronze')
        self.s3.create_bucket.assert_not_called()

    def test_loan_topic_uses_documented_plural_path(self):
        from datetime import datetime

        key = writer_module.get_s3_key('loan-events', datetime(2026, 9, 21))
        self.assertTrue(key.startswith('loans/year=2026/month=09/day=21/'))

    def test_missing_bucket_is_created(self):
        self.s3.head_bucket.side_effect = ClientError('404')
        writer_module.create_bucket_if_not_exists(self.s3, 'bronze')
        self.s3.create_bucket.assert_called_once_with(Bucket='bronze')

    def test_main_disables_automatic_acknowledgment(self):
        with patch.object(writer_module, 'create_s3_client', return_value=self.s3), \
             patch.object(writer_module, 'Consumer', return_value=self.consumer) as factory, \
             patch.object(writer_module.BronzeWriter, 'run'):
            writer_module.main()
        config = factory.call_args.args[0]
        self.assertIs(config['enable.auto.commit'], False)
        self.assertIs(config['enable.auto.offset.store'], False)


if __name__ == '__main__':
    unittest.main()
