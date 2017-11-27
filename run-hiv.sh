#!/usr/bin/env bash

pushd `dirname $0` > /dev/null
SCRIPT_PATH="$( cd "$(dirname "$0")" ; pwd -P )"
popd > /dev/null

DIR_NAME=$(basename $SCRIPT_PATH)

docker run \
    -e LOGGING_CONFIG=classpath:logback-docker.xml \
    -e HIV_HENDELSER_PER_REQUEST=10 \
    -e HIV_INITIALIZE=false \
    -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
    -e SCHEMA_REGISTRY_URL=http://schema_registry:8081 \
    -e SKATT_API_URL=http://testapi:8080/ekstern/skatt/datasamarbeid/api/ \
    --network=${DIR_NAME}_default \
    navikt/tortuga-hiv