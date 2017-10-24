#!/usr/bin/env bash

pushd `dirname $0` > /dev/null
SCRIPT_PATH="$( cd "$(dirname "$0")" ; pwd -P )"
popd > /dev/null

DIR_NAME=$(basename $SCRIPT_PATH)

docker run \
    -e LOGGING_CONFIG=classpath:logback-docker.xml \
    -e SPRING_DATASOURCE_URL=jdbc:h2:file:/hiv/hivdb \
    -e SPRING_JPA_HIBERNATE_DDL_AUTO=update \
    -e HIV_CHUNK_SIZE=1 \
    -v ${SCRIPT_PATH}/hiv:/hiv \
    --link ${DIR_NAME}_kafka_1:kafka \
    --link ${DIR_NAME}_testapi_1:testapi \
    --network=${DIR_NAME}_default \
    navikt/tortuga-hiv