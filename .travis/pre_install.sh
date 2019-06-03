#!/usr/bin/env bash
apt-get clean
apt-get update -q
apt-get install -y --no-install-recommends apt-utils wget gnupg software-properties-common
apt-get install -y apt-transport-https ca-certificates
wget -qO - https://packages.confluent.io/deb/5.1/archive.key | apt-key add -
add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/5.1 stable main"
apt-get update
apt-get install -y librdkafka-dev

# a hack to overwrite /usr/include/librdkafka/rdkafka.h
# since "RD_KAFKA_RESP_ERR__FATAL" is not present in librdkafka-dev v0.11.6
# even the confluent kafka claims it requires version >= v0.11.5
sudo cp -rf thirdparty/librdkafka/src/rdkafka.h /usr/include/librdkafka/rdkafka.h

