Test API
=========

Dette API-et skal i best mulig grad speile [Skatteetaten sitt API](https://skatteetaten.github.io/datasamarbeid-api-dokumentasjon/reference_pgi.html).
APIet vil svare med tilfeldige data, og vil også **feile tilfeldig**.

## Endepunkter

### VarslingsAPI

Endepunkt: `/varsel`

Svarer med en tilfeldig liste over inntekter med endringer.

```json
[
	{
		"endret":"2016-01-01 00:00:00",
		"personindentfikator":"07095912345",
		"inntektsaar":"2017"
	},
	{
		"endret":"2016-01-01 00:00:00",
		"personindentfikator":"26127812345",
		"inntektsaar":"2017"
	}
]
```

### PGI-API

Endepunkt: `/pensjonsgivendeinntekt`

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