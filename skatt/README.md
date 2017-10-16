Skatt
=====

Inneholder DTO-er som beskriver responsen fra Skatteetaten sine API-er og REST-klienter for å konsumere disse.

## Konfigurasjon

Hendelse- og Inntektsbønnene konfigureres via:

```
skatt.api.url=http://testapi:8080
skatt.api.hendelser-path=/api/formueinntekt/beregnetskatt/hendelser
skatt.api.pgi-path=/api/formueinntekt/pensjonsgivendeinntekt
```

Disse kan selvfølgelig også spesifiseres som miljøvariabler, `SKATT_API_URL`, `SKATT_API_HENDELSER_PATH` osv.