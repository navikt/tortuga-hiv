#!/usr/bin/env bash

pushd `dirname $0` > /dev/null
SCRIPT_PATH="$( cd "$(dirname "$0")" ; pwd -P )"
popd > /dev/null

DIR_NAME=$(basename $SCRIPT_PATH)

docker run \
    -p 8086:8080 \
    -e LOGGING_CONFIG=classpath:logback-docker.xml \
    -e HIV_HENDELSER_PER_REQUEST=1000 \
    -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
    -e SCHEMA_REGISTRY_URL=http://schema_registry:8081 \
    -e KAFKA_SASL_JAAS_CONFIG= \
    -e KAFKA_SASL_MECHANISM= \
    -e KAFKA_SECURITY_PROTOCOL= \
    -e SKATT_API_URL=http://testapi:8080/ekstern/skatt/datasamarbeid/api/formueinntekt/beregnetskatt/ \
    --network=${DIR_NAME}_default \
    repo.adeo.no:5443/tortuga-hiv
