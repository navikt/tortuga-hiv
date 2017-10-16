FROM navikt/java:8

RUN apk --no-cache add curl

HEALTHCHECK --start-period=30s CMD curl --fail http://localhost:8080/isAlive || exit 1