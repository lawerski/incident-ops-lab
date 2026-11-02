import json
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from app.api import make_handler


class MemoryRepository:
    def __init__(self):
        self.items = {}
        self.up = True

    def create(self, document):
        if not self.up:
            raise ConnectionError("down")
        self.items[document["_id"]] = document

    def get(self, incident_id):
        return self.items.get(incident_id)

    def ping(self):
        if not self.up:
            raise ConnectionError("down")

    def count_pending(self):
        return len(self.items)


class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repository = MemoryRepository()
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(cls.repository))
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def setUp(self):
        self.repository.up = True
        self.repository.items.clear()

    def test_post_get_and_readiness(self):
        request = Request(self.base + "/incidents", method="POST",
                          data=json.dumps({"service": "api", "summary": "HTTP 503",
                                           "severity": "high"}).encode(),
                          headers={"Content-Type": "application/json"})
        with urlopen(request) as response:
            self.assertEqual(201, response.status)
            incident = json.load(response)
            self.assertEqual("queued", incident["delivery"])
        with urlopen(self.base + f"/incidents/{incident['id']}") as response:
            self.assertEqual("api", json.load(response)["service"])
        with urlopen(self.base + "/health/ready") as response:
            self.assertEqual(200, response.status)
        with urlopen(self.base + "/metrics") as response:
            self.assertIn("incident_outbox_pending 1", response.read().decode())

    def test_invalid_input_and_database_outage(self):
        request = Request(self.base + "/incidents", method="POST", data=b"{}",
                          headers={"Content-Type": "application/json"})
        with self.assertRaises(HTTPError) as error:
            urlopen(request)
        self.assertEqual(400, error.exception.code)
        self.repository.up = False
        with self.assertRaises(HTTPError) as error:
            urlopen(self.base + "/health/ready")
        self.assertEqual(503, error.exception.code)
        with urlopen(self.base + "/health/live") as response:
            self.assertEqual(200, response.status)


if __name__ == "__main__":
    unittest.main()
