Prosjekt Tortuga
================

Tortuga er NAV sitt nye system for å hente informasjon om pensjonsgivende inntekt
fra [Skatteetaten sitt nye grensesnitt](https://skatteetaten.github.io/datasamarbeid-api-dokumentasjon/reference_pgi.html).

Prosjektet består foreløpig av to separate applikasjoner, Hiv og Hoi.

## Hiv

Hiv skal **H**åndtere **I**nntekts**v**arslinger om pensjonsgivende inntekt fra Skatteetaten.

## Hoi

Hoi skal **H**ente **O**pplysninger om **I**nntekt til personer og muliggjøre persistering av informasjonen.

## Installasjon og kjøring

### Bygging

For å bygge JAR og tilhørende Docker images:

```
make
```

Se de øvrige modulene for utfyllende informasjon om deres bygg- og kjøretidsmiljø.

## Testing

For å sette opp et testmiljø holder det å kjøre Docker Compose: 

```
docker-compose up
```

Hiv kan da startes med `./run-hiv.sh`, og tilsvarende for Hoi med `./run-hoi.sh`.

Testapiet er satt opp med lokal portmapping `8082`, og du kan opprette testhendelser slik:

```
curl -X POST http://localhost:8082/createHendelser/<antall hendelser>
```

---

# Henvendelser

Spørsmål knyttet til koden eller prosjektet kan rettes mot:

* David Steinsland, david.steinsland@nav.no

## For NAV-ansatte

Interne henvendelser kan sendes via Slack i kanalen #peon.
