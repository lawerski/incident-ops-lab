# Wariant Kubernetes

Pliki w `k8s/` wdrażają trzy procesy aplikacji: API, publisher i indexer. MongoDB, Kafka i Elasticsearch nie są częścią manifestów. Potrzebny jest klaster z osiągalnymi adresami tych usług, utworzonymi tematami `incidents.v1` i `incidents.dlq` oraz obraz aplikacji w rejestrze dostępnym dla klastra.

## Przygotowanie

1. Zbuduj obraz (`docker build -t <registry>/incident-ops-lab:<tag> .`), wypchnij go do rejestru i zamień `incident-ops-lab:dev` w trzech manifestach na rzeczywisty adres obrazu.
2. Ustaw adresy `KAFKA_BOOTSTRAP_SERVERS` i `ELASTICSEARCH_URL` w `k8s/config.yaml`. Obecne domeny `.example.internal` są przykładami.
3. Utwórz namespace i sekret z adresem MongoDB. Nie zapisuj hasła w repozytorium:

```bash
kubectl apply -f k8s/namespace.yaml
kubectl -n incident-ops create secret generic incident-ops-secrets --from-literal=MONGO_URI='mongodb://USER:HASLO@mongo.example.internal:27017/incident_ops?authSource=admin'
kubectl apply -f k8s/config.yaml
kubectl apply -f k8s/api.yaml -f k8s/publisher.yaml -f k8s/indexer.yaml
kubectl -n incident-ops get pods,svc
```

Wstaw własne wartości przed uruchomieniem polecenia z sekretem; przykład z historii powłoki nie jest dobrym sposobem przekazywania prawdziwego hasła. W praktycznym klastrze użyj firmowego menedżera sekretów, uwierzytelniania i TLS dla wszystkich usług.

## Sprawdzenie wdrożenia

```bash
kubectl -n incident-ops rollout status deployment/incident-api
kubectl -n incident-ops logs deployment/incident-publisher --tail=100
kubectl -n incident-ops logs deployment/incident-indexer --tail=100
kubectl -n incident-ops port-forward svc/incident-api 8080:80
```

W drugim terminalu sprawdź `curl http://127.0.0.1:8080/health/ready`. Liveness sprawdza proces HTTP, readiness dostęp do MongoDB. Kafka nie jest częścią readiness API, ponieważ zgłoszenia trafiają najpierw do MongoDB. Gdy broker wróci, publisher wyśle zaległe rekordy. API można zwiększyć do kilku replik; publisher pozostaje jedną repliką bez mechanizmu claimowania outboxu.

Te manifesty pokazują sposób wdrożenia i diagnozowania. Nie zostały przetestowane na działającym klastrze w tym repozytorium. Dla ruchu produkcyjnego trzeba dodać ingress z uwierzytelnianiem, bezpieczne połączenia, alerty, retencję, kopie zapasowe i testy obciążeniowe.
