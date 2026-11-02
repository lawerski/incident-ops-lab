"""MongoDB stores incidents and acts as a small transactional outbox."""

import os
from datetime import datetime, timezone

from pymongo import MongoClient
from pymongo import ASCENDING


class IncidentRepository:
    def __init__(self, uri=None):
        self.client = MongoClient(
            uri or os.environ.get("MONGO_URI", "mongodb://mongo:27017"),
            serverSelectionTimeoutMS=2000,
        )
        self.collection = self.client["incident_ops"]["incidents"]

    def ping(self):
        self.client.admin.command("ping")

    def ensure_indexes(self):
        self.collection.create_index([("published_at", ASCENDING), ("created_at", ASCENDING)])

    def create(self, document):
        self.collection.insert_one(document)

    def get(self, incident_id):
        return self.collection.find_one({"_id": incident_id})

    def pending(self, limit=20):
        return list(
            self.collection.find({"published_at": None})
            .sort("created_at", 1)
            .limit(limit)
        )

    def mark_published(self, incident_id):
        now = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        self.collection.update_one(
            {"_id": incident_id, "published_at": None},
            {"$set": {"published_at": now}},
        )

    def count_pending(self):
        return self.collection.count_documents({"published_at": None})
