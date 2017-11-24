FROM busybox

WORKDIR /app

ARG JAR_FILE
COPY target/${JAR_FILE} /app/app.jar

FROM navikt/java:8
COPY --from=0 /app/app.jar .