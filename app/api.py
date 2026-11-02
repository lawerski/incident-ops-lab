"""Small HTTP API. MongoDB is the only synchronous dependency of POST."""

import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import UUID

from app.incidents import as_response, new_incident


MAX_BODY = 8192
LOG = logging.getLogger(__name__)


def make_handler(repository):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, status, payload, headers=None):
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("X-Content-Type-Options", "nosniff")
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/health/live":
                return self._send(200, {"status": "ok"})
            if self.path == "/health/ready":
                try:
                    repository.ping()
                    return self._send(200, {"status": "ready"})
                except Exception:
                    LOG.exception("MongoDB readiness check failed")
                    return self._send(503, {"error": "MongoDB unavailable"})
            if self.path == "/metrics":
                try:
                    pending = repository.count_pending()
                except Exception:
                    LOG.exception("MongoDB metrics check failed")
                    return self._send(503, {"error": "MongoDB unavailable"})
                body = (
                    "# HELP incident_outbox_pending Incidents waiting for Kafka delivery.\n"
                    "# TYPE incident_outbox_pending gauge\n"
                    f"incident_outbox_pending {pending}\n"
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                return self.wfile.write(body)
            if self.path.startswith("/incidents/"):
                incident_id = self.path[len("/incidents/"):]
                try:
                    UUID(incident_id)
                except ValueError:
                    return self._send(404, {"error": "Incident not found"})
                try:
                    incident = repository.get(incident_id)
                except Exception:
                    LOG.exception("MongoDB lookup failed")
                    return self._send(503, {"error": "MongoDB unavailable"})
                if incident is None:
                    return self._send(404, {"error": "Incident not found"})
                return self._send(200, as_response(incident))
            return self._send(404, {"error": "Not found"})

        def do_POST(self):
            if self.path != "/incidents":
                return self._send(404, {"error": "Not found"})
            if self.headers.get_content_type() != "application/json":
                return self._send(415, {"error": "Content-Type must be application/json"})
            try:
                size = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                return self._send(400, {"error": "Invalid Content-Length"})
            if size < 1 or size > MAX_BODY:
                return self._send(413, {"error": "Body must be between 1 and 8192 bytes"})
            try:
                payload = json.loads(self.rfile.read(size))
                incident = new_incident(payload)
            except (ValueError, UnicodeDecodeError) as exc:
                return self._send(400, {"error": str(exc)})
            try:
                repository.create(incident)
            except Exception:
                LOG.exception("MongoDB insert failed")
                return self._send(503, {"error": "MongoDB unavailable"})
            return self._send(201, as_response(incident), {"Location": f"/incidents/{incident['_id']}"})

    return Handler


def main():
    from app.storage import IncidentRepository

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    server = ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("PORT", "8080"))),
                                 make_handler(IncidentRepository()))
    LOG.info("API listening on port %s", server.server_port)
    server.serve_forever()


if __name__ == "__main__":
    main()
