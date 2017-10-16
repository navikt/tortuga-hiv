Test API
=========

Dette API-et skal i best mulig grad speile [Skatteetaten sitt API](https://skatteetaten.github.io/datasamarbeid-api-dokumentasjon/reference_pgi.html).
APIet vil svare med tilfeldige data, og vil også **feile tilfeldig**.

## Endepunkter

### VarslingsAPI

Endepunkt: `/api/formueinntekt/beregnetskatt/hendelser`

Svarer med en tilfeldig liste over inntekter med endringer.

```json
{
  "hendelser": [
    {
      "sekvensnummer": 1,
      "identifikator": "09048000875",
      "gjelderPeriode": "2015"
    },
    {
      "sekvensnummer": 2,
      "identifikator": "20125001158",
      "gjelderPeriode": "2015"
    }
  ]
}
```

### PGI-API

Endepunkt: `/api/formueinntekt/pensjonsgivendeinntekt/{inntektsaar}/{personidentifikator}`

Svarer med en tilfeldig inntekt:

```json
{
	"personindentfikator":"12345678901",
	"inntektsaar":"2016",
	"pensjonsgivendeInntekt":623900.908082498
}
```

## Testdata

Ved oppstart vil Hibernate automatisk opprette en database basert på klasser annotert med `@Entity` og utføre SQL-ene definert i `src/main/resources/import.sql`.