"""Validation and event format shared by the API and workers."""

from datetime import datetime, timezone
from uuid import uuid4


SEVERITIES = {"low", "medium", "high", "critical"}


def _required_text(data, key, maximum):
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Pole {key} musi być niepustym tekstem")
    value = value.strip()
    if len(value) > maximum:
        raise ValueError(f"Pole {key} może mieć maksymalnie {maximum} znaków")
    return value


def new_incident(data):
    if not isinstance(data, dict):
        raise ValueError("Oczekiwano obiektu JSON")
    service = _required_text(data, "service", 80)
    summary = _required_text(data, "summary", 200)
    severity = data.get("severity")
    if not isinstance(severity, str) or severity not in SEVERITIES:
        raise ValueError("severity: low, medium, high albo critical")
    details = data.get("details", "")
    if not isinstance(details, str) or len(details) > 2000:
        raise ValueError("details musi być tekstem do 2000 znaków")
    return {
        "_id": str(uuid4()),
        "service": service,
        "summary": summary,
        "severity": severity,
        "details": details.strip(),
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "published_at": None,
    }


def as_event(document):
    return {"id": document["_id"], **{
        key: document[key] for key in ("service", "summary", "severity", "details", "created_at")
    }}


def as_response(document):
    return {
        **as_event(document),
        "delivery": "published_to_kafka" if document.get("published_at") else "queued",
    }


def validate_event(event):
    if not isinstance(event, dict):
        raise ValueError("Zdarzenie nie jest obiektem")
    for key in ("id", "service", "summary", "created_at"):
        if not isinstance(event.get(key), str) or not event[key]:
            raise ValueError(f"Brak lub niepoprawne pole {key}")
    if not isinstance(event.get("severity"), str) or event["severity"] not in SEVERITIES:
        raise ValueError("Niepoprawne severity")
    if not isinstance(event.get("details"), str):
        raise ValueError("Niepoprawne details")
    return event
