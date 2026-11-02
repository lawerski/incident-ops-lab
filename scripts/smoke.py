"""Minimal end-to-end check after docker compose up -d --build."""

import json
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BASE = "http://127.0.0.1:8080"
ELASTIC = "http://127.0.0.1:9200"


def get_json(url):
    with urlopen(url, timeout=5) as response:
        return json.load(response)


def main():
    request = Request(BASE + "/incidents", method="POST",
                      data=json.dumps({"service": "checkout", "severity": "high",
                                       "summary": "Wzrost odpowiedzi HTTP 503",
                                       "details": "Przykładowe zgłoszenie testowe"}).encode(),
                      headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=5) as response:
        incident = json.load(response)
        assert response.status == 201
    incident_id = incident["id"]
    print(f"Created incident: {incident_id}")

    for _ in range(30):
        try:
            document = get_json(f"{ELASTIC}/incidents-v1/_doc/{incident_id}")
            if document.get("found") and document.get("_source", {}).get("id") == incident_id:
                print("Indexed in Elasticsearch: OK")
                return
        except (HTTPError, URLError):
            pass
        time.sleep(2)
    raise SystemExit("Record not indexed within 60s; inspect docker compose logs publisher indexer")


if __name__ == "__main__":
    main()
