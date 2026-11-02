# Runbook: zgłoszenia i indeksowanie

## Kontrole na początku dyżuru

```bash
docker compose ps
curl -fsS http://127.0.0.1:8080/health/live
curl -fsS http://127.0.0.1:8080/health/ready
curl -fsS http://127.0.0.1:8080/metrics
```

`/health/live` sprawdza proces. `/health/ready` pyta MongoDB, bo bez bazy API nie może zapisać zgłoszenia. Metryka `incident_outbox_pending` pokazuje liczbę rekordów, które nie otrzymały jeszcze potwierdzenia z Kafki. Samo jej chwilowe zwiększenie jest normalne; utrzymujący się wzrost wymaga analizy.

## API zwraca 503

1. Sprawdź `/health/ready`, `docker compose ps mongo api` i `docker compose logs --tail=100 mongo api`.
2. Potwierdź, czy MongoDB odpowiada i ma dostępne miejsce na dane. Przed restartem sprawdź komunikaty o błędach; restart nie naprawi pełnego dysku.
3. Po przywróceniu bazy ponów zapis. API zwraca 201 tylko po zapisie w MongoDB.

## Zaległe rekordy w outboxie

1. Odczytaj `/metrics` dwukrotnie w odstępie minuty.
2. Sprawdź `docker compose logs --tail=100 publisher kafka` oraz `docker compose ps kafka-init`.
3. Potwierdź istnienie tematów:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list
```

4. Po naprawie połączenia publisher automatycznie ponawia wysyłkę. Obserwuj, czy `incident_outbox_pending` maleje. Jedna instancja publishera działa celowo: więcej instancji wymagałoby blokowania/claimowania rekordów.

## Kafka działa, ale Kibana nie pokazuje zdarzeń

1. Sprawdź `docker compose logs --tail=100 indexer elasticsearch kibana`.
2. Zweryfikuj obecność indeksu: `curl -fsS http://127.0.0.1:9200/_cat/indices/incidents-v1?v`.
3. Sprawdź w Kibanie Data View `incidents-v1`, pole czasu `created_at` i zakres czasu w Discover.
4. Jeżeli Elasticsearch było niedostępne, indexer ponowi zapis przed zatwierdzeniem offsetu Kafki. Ponowny zapis tego samego `id` nadpisze dokument o tym samym `_id`.
5. Jeżeli zdarzenie było niepoprawne, odczytaj `incidents.dlq` i przeanalizuj pole `reason`; surowe dane są zakodowane jako base64:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic incidents.dlq --from-beginning --timeout-ms 5000
```

## Ćwiczenie odtwarzania

1. Uruchom Compose, zapisz identyfikator zgłoszenia i sprawdź, czy pojawiło się w indeksie.
2. Zatrzymaj `kafka`: `docker compose stop kafka`.
3. Wyślij kolejne zgłoszenie, zapisz jego `id` i potwierdź, że `/metrics` wzrosło.
4. Wznów usługę: `docker compose start kafka`. Publisher powinien wysłać zaległe rekordy.
5. Zapytaj `GET /incidents/{id}` oraz `GET http://127.0.0.1:9200/incidents-v1/_doc/{id}`. Zweryfikuj ten sam identyfikator i treść.

Nie używaj w demonstracji prawdziwych danych klientów, haseł ani tokenów. W tym Compose nie ma autoryzacji API i Elasticsearch, a porty nasłuchują tylko lokalnie.
