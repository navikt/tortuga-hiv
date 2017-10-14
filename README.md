Prosjekt Tortuga [![Build Status](https://travis-ci.org/navikt/tortuga.svg?branch=master)](https://travis-ci.org/navikt/tortuga)
================

Tortuga er NAV sitt nye system for å hente informasjon om pensjonsgivende inntekt
fra [Skatteetaten sitt nye grensesnitt](https://skatteetaten.github.io/datasamarbeid-api-dokumentasjon/reference_pgi.html).

Prosjektet består foreløpig av to separate applikasjoner, Hiv og Hoi.

## Hiv

Hiv skal **H**ente **I**nn **V**arslinger om pensjonsgivende inntekt fra Skatteetaten.

## Hoi

Hoi skal **H**ente **O**pplysninger om **I**nntekt til personer og muliggjøre persistering av informasjonen.

## Installasjon og kjøring

Vi bruker [spotify/dockerfile-maven](https://github.com/spotify/dockerfile-maven) for å bygge Docker images i lag med Maven-bygget.

### Bygging

For å bygge JAR og tilhørende Docker images:

```
./mvnw clean verify
```

Se de øvrige modulene for utfyllende informasjon om deres bygg- og kjøretidsmiljø.

### Deploy

Deploy Docker images til DockerHub (JAR blir ikke deployet):

```
./mvnw deploy
```

Dette vil riktignok kjøre et fullt bygg og viss en bare vil pushe Docker images, kan dette gjøres slik:

```
./mvnw com.spotify:dockerfile-maven-plugin:push -pl 'hiv,hoi,testapi'
```

## Testing

For å sette opp et testmiljø holder det å kjøre Docker Compose: 

```
docker-compose up
```