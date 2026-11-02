"""Consume Kafka records, index them by incident ID, then commit offsets."""

import base64
import json
import logging
import os
import time

from app.incidents import validate_event
from app.search import ElasticsearchSink


LOG = logging.getLogger(__name__)


def process_record(raw, sink, send_dlq):
    try:
        event = validate_event(json.loads(raw))
    except (ValueError, TypeError, UnicodeDecodeError) as exc:
        send_dlq({"raw_base64": base64.b64encode(raw or b"").decode("ascii"),
                  "reason": str(exc)[:200]})
        return "dlq"
    sink.index(event)
    return "indexed"


def main():
    from confluent_kafka import Consumer, Producer

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    broker = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    topic = os.environ.get("KAFKA_TOPIC", "incidents.v1")
    dlq_topic = os.environ.get("DLQ_TOPIC", "incidents.dlq")
    sink = ElasticsearchSink()
    dlq_producer = Producer({"bootstrap.servers": broker})

    def send_dlq(payload):
        errors = []
        dlq_producer.produce(dlq_topic, value=json.dumps(payload).encode("utf-8"),
                             on_delivery=lambda error, _message: errors.append(error) if error else None)
        remaining = dlq_producer.flush(10)
        if remaining or errors:
            raise RuntimeError(f"DLQ delivery failed: {errors or remaining}")

    while True:
        consumer = None
        try:
            sink.ensure_index()
            consumer = Consumer({
                "bootstrap.servers": broker,
                "group.id": "incident-elasticsearch-indexer",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })
            consumer.subscribe([topic])
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(message.error())
                result = process_record(message.value(), sink, send_dlq)
                consumer.commit(message=message, asynchronous=False)
                LOG.info("Record offset=%s result=%s", message.offset(), result)
        except Exception:
            LOG.exception("Indexer failed; retrying in 5 seconds")
            time.sleep(5)
        finally:
            if consumer is not None:
                consumer.close()


if __name__ == "__main__":
    main()
