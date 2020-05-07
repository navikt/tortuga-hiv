Tortuga HIV
===========

HIV skal lytte på hendelser om beregnet skatt og tilby disse på en Kafka-topic.

## Testing

For å sette opp et testmiljø (Kafka-kluster) holder det å kjøre Docker Compose: 

```
docker-compose up
```

Man kan da kjøre både HIV og HOI mot testmiljøet.

HIV kan startes med:

```
docker run \
    -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
    -e SCHEMA_REGISTRY_URL=http://schema_registry:8081 \
    -e KAFKA_SASL_JAAS_CONFIG= \
    -e KAFKA_SASL_MECHANISM= \
    -e KAFKA_SECURITY_PROTOCOL= \
    -e SKATT_API_URL=http://testapi:8080/ekstern/skatt/datasamarbeid/api/formueinntekt/beregnetskatt/ \
    --network=tortuga_default \
    repo.adeo.no:5443/tortuga-hiv
```

HOI kan startes med:

```
docker run \
    --network=tortgua_default \
    repo.adeo.no:5443/tortuga-hoi
```

NB: I eksemplene ovenfor forutsettes det at du har klonet prosjektet ned til mappen `tortuga`,
ettersom dette påvirker navnet Docker gir til nettverket.

Testapiet er satt opp med lokal portmapping `8082`, og du kan opprette testhendelser slik:

```
curl -X POST http://localhost:8082/createHendelser/<antall hendelser>
```

---

# Henvendelser

Spørsmål knyttet til koden eller prosjektet kan stilles som issues her på GitHub.

## For NAV-ansatte

Interne henvendelser kan sendes via Slack i kanalen #peon.
