import json
import unittest

from app.incidents import as_event, new_incident, validate_event
from app.indexer import process_record
from app.publisher import publish_batch


class FakeRepository:
    def __init__(self, document):
        self.document = document
        self.marked = []

    def pending(self):
        return [self.document]

    def mark_published(self, incident_id):
        self.marked.append(incident_id)


class FakeSink:
    def __init__(self, fail=False):
        self.fail = fail
        self.indexed = []

    def index(self, event):
        if self.fail:
            raise ConnectionError("Elasticsearch unavailable")
        self.indexed.append(event)


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.document = new_incident({"service": "billing", "summary": "HTTP 503",
                                      "severity": "high", "details": "Po wdrożeniu"})

    def test_publisher_marks_document_only_after_broker_ack(self):
        repository = FakeRepository(self.document)

        def fail(_id, _event):
            raise ConnectionError("Kafka unavailable")

        with self.assertRaises(ConnectionError):
            publish_batch(repository, fail)
        self.assertEqual([], repository.marked)

        sent = []
        self.assertEqual(1, publish_batch(repository, lambda id, event: sent.append((id, event))))
        self.assertEqual([self.document["_id"]], repository.marked)
        self.assertEqual(self.document["_id"], sent[0][1]["id"])

    def test_indexer_retries_when_search_is_down(self):
        raw = json.dumps(as_event(self.document)).encode()
        with self.assertRaises(ConnectionError):
            process_record(raw, FakeSink(fail=True), lambda _event: None)

        sink = FakeSink()
        self.assertEqual("indexed", process_record(raw, sink, lambda _event: None))
        self.assertEqual(self.document["_id"], sink.indexed[0]["id"])

    def test_invalid_record_goes_to_dlq(self):
        dlq = []
        self.assertEqual("dlq", process_record(b"not json", FakeSink(), dlq.append))
        self.assertIn("raw_base64", dlq[0])

    def test_dlq_error_is_retried(self):
        def fail(_event):
            raise ConnectionError("Kafka unavailable")

        with self.assertRaises(ConnectionError):
            process_record(b"not json", FakeSink(), fail)

    def test_validation_rejects_empty_summary_and_bad_severity(self):
        with self.assertRaises(ValueError):
            new_incident({"service": "billing", "summary": " ", "severity": "low"})
        with self.assertRaises(ValueError):
            new_incident({"service": "billing", "summary": "test", "severity": "urgent"})
        with self.assertRaises(ValueError):
            new_incident({"service": "billing", "summary": "test", "severity": ["high"]})
        with self.assertRaises(ValueError):
            validate_event({"id": "x", "severity": "high"})


if __name__ == "__main__":
    unittest.main()
