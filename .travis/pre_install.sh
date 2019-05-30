#!/usr/bin/env bash
apt-get clean
apt-get update -q
# a hack to overwrite /usr/include/librdkafka/rdkafka.h
# since "RD_KAFKA_RESP_ERR__FATAL" is not present in librdkafka-dev v0.11.6
# even the confluent kafka claims it requires version >= v0.11.5
cp -rf thirdparty/librdkafka/src/rdkafka.h /usr/include/librdkafka/rdkafka.h

