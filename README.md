Prosjekt Tortuga [![Build Status](https://travis-ci.org/navikt/tortuga.svg?branch=master)](https://travis-ci.org/navikt/tortuga)
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

Hiv kan da kjøres med `./run-hiv.sh` så mange ganger som ønskelig. 