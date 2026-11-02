"""Publish pending MongoDB documents to Kafka; retry if the broker is down."""

import json
import logging
import os
import time

from app.incidents import as_event


LOG = logging.getLogger(__name__)


def publish_batch(repository, send):
    count = 0
    for document in repository.pending():
        send(document["_id"], as_event(document))
        repository.mark_published(document["_id"])
        count += 1
    return count


def main():
    from confluent_kafka import Producer
    from app.storage import IncidentRepository

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    repository = IncidentRepository()
    topic = os.environ.get("KAFKA_TOPIC", "incidents.v1")
    producer = Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")})

    def send(incident_id, event):
        errors = []

        def delivered(error, _message):
            if error:
                errors.append(error)

        producer.produce(topic, key=incident_id.encode("ascii"),
                         value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
                         on_delivery=delivered)
        remaining = producer.flush(10)
        if remaining or errors:
            raise RuntimeError(f"Kafka delivery failed: {errors or remaining}")

    index_ready = False
    while True:
        try:
            if not index_ready:
                repository.ensure_indexes()
                index_ready = True
            count = publish_batch(repository, send)
            if count:
                LOG.info("Published %s incidents", count)
            time.sleep(2 if count else 5)
        except Exception:
            LOG.exception("Publisher failed; retrying in 5 seconds")
            time.sleep(5)


if __name__ == "__main__":
    main()
