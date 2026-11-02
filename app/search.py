"""Minimal Elasticsearch REST client for one index."""

import json
import os
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen


INDEX = "incidents-v1"
MAPPING = {
    "mappings": {"properties": {
        "id": {"type": "keyword"},
        "service": {"type": "keyword"},
        "severity": {"type": "keyword"},
        "summary": {"type": "text"},
        "details": {"type": "text"},
        "created_at": {"type": "date"},
    }}
}


class ElasticsearchSink:
    def __init__(self, url=None):
        self.url = (url or os.environ.get("ELASTICSEARCH_URL", "http://elasticsearch:9200")).rstrip("/")

    def _request(self, method, path, body=None):
        raw = json.dumps(body).encode("utf-8") if body is not None else None
        request = Request(self.url + path, data=raw, method=method,
                          headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=5) as response:
            return response.status

    def ensure_index(self):
        try:
            self._request("HEAD", f"/{INDEX}")
            return
        except HTTPError as exc:
            if exc.code != 404:
                raise
        try:
            self._request("PUT", f"/{INDEX}", MAPPING)
        except HTTPError as exc:
            # A second worker may have created the same index in the meantime.
            if exc.code != 400 or b"resource_already_exists_exception" not in exc.read():
                raise

    def index(self, event):
        self._request("PUT", f"/{INDEX}/_doc/{quote(event['id'], safe='')}", event)
