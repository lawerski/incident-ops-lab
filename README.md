# Incident Ops Lab

Mały projekt do przećwiczenia utrzymania aplikacji i obsługi incydentów. API przyjmuje zgłoszenie, MongoDB przechowuje je do czasu wysłania, Kafka przenosi zdarzenie, a worker zapisuje je w Elasticsearch. W Kibanie można przeglądać zgłoszenia po usłudze, wadze i czasie.

Celowo jest to środowisko demonstracyjne, a nie system produkcyjny. Porty są udostępnione wyłącznie na `127.0.0.1`; w Compose wyłączone jest uwierzytelnianie Elasticsearch i nie ma uwierzytelniania API. Nie należy wystawiać tych usług do Internetu.

## Jak to działa

```text
POST /incidents → MongoDB (zgłoszenie + kolejka do wysłania)
                         ↓ publisher
                    Kafka: incidents.v1
                         ↓ indexer
               Elasticsearch: incidents-v1 → Kibana
```

API potwierdza zapis w MongoDB. Dostarczenie do Kafki dzieje się później: przerwa w pracy brokera nie blokuje przyjmowania nowych zgłoszeń. Publisher oznacza dokument jako wysłany dopiero po potwierdzeniu z Kafki. Indexer zatwierdza offset dopiero po zapisie w Elasticsearch. Po awarii może dojść do ponownego przetworzenia, dlatego dokument w indeksie ma identyfikator zgłoszenia jako `_id`. Uszkodzone zdarzenia trafiają do `incidents.dlq`.

## Uruchomienie

Wymagane: Docker z Compose i kilka GB wolnej pamięci dla Kafki oraz Elastic Stack.

```bash
docker compose up -d --build
docker compose ps
python3 scripts/smoke.py
```

Pierwszy start może potrwać parę minut. API: `http://127.0.0.1:8080`, Kibana: `http://127.0.0.1:5601`. Smoke test tworzy zgłoszenie i czeka na dokument w Elasticsearch. Aby zatrzymać środowisko: `docker compose down`. Polecenie `docker compose down -v` usuwa również dane.

Przykładowe zgłoszenie:

```bash
curl -sS -X POST http://127.0.0.1:8080/incidents \
  -H 'Content-Type: application/json' \
  -d '{"service":"checkout","severity":"high","summary":"HTTP 503 po wdrożeniu","details":"Błąd dla części żądań"}'
```

Odpowiedź zawiera `id` i `delivery`. `queued` oznacza oczekiwanie na Kafkę, `published_to_kafka` oznacza potwierdzone przekazanie do brokera. To drugie nie oznacza jeszcze, że rekord jest już widoczny w Kibanie.

W Kibanie utwórz Data View `incidents-v1` z polem czasu `created_at`, a następnie otwórz **Discover**. Możesz filtrować po `service` lub `severity`.

## Endpointy

| Metoda | Ścieżka | Cel |
| --- | --- | --- |
| `POST` | `/incidents` | Zapis zgłoszenia (`service`, `severity`, `summary`, opcjonalne `details`) |
| `GET` | `/incidents/{id}` | Stan zapisu i wysłania zgłoszenia |
| `GET` | `/health/live` | Czy proces API odpowiada |
| `GET` | `/health/ready` | Czy API ma dostęp do MongoDB |
| `GET` | `/metrics` | Liczba zgłoszeń czekających na Kafkę w formacie Prometheus |

Wagi zgłoszeń: `low`, `medium`, `high`, `critical`. API przyjmuje maksymalnie 8 KiB JSON; szczegóły mają limit 2000 znaków.

## Testy i ćwiczenie awarii

```bash
python3 -m unittest discover -s tests -v
docker compose stop kafka
# wyślij nowe zgłoszenie przez API, sprawdź /metrics
docker compose start kafka
python3 scripts/smoke.py
```

Testy jednostkowe oraz test HTTP korzystają z pamięciowych zamienników, więc uruchamiają się bez Dockera. Test `scripts/smoke.py` wymaga całego środowiska. Dalsze scenariusze i polecenia diagnostyczne są w [runbooku](docs/runbook.md). Manifesty i ograniczenia wariantu Kubernetes opisuje [instrukcja](docs/kubernetes.md).

## Co pokazuje repozytorium

- Docker Compose do lokalnego odtworzenia przepływu MongoDB → Kafka → Elasticsearch/Kibana.
- Rozdzielenie liveness i readiness; metrykę zaległych zgłoszeń oraz procedurę reakcji na awarię.
- Manifesty Kubernetes z zasobami, ograniczeniami procesu i wdrożeniem API bez przerwy w dostępności.
- Testy błędów i prosty CI uruchamiający testy oraz budujący obraz.

W środowisku produkcyjnym potrzebne byłyby m.in. autoryzacja, TLS, kopie zapasowe, alerty, monitoring opóźnienia konsumenta i zarządzanie sekretami. Manifesty K8s obejmują aplikację; zakładają wcześniej przygotowane usługi MongoDB, Kafka i Elasticsearch.
