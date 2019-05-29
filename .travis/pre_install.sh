#!/usr/bin/env bash
apt-get clean
apt-get update -q
apt-get install -y --no-install-recommends apt-utils wget gnupg software-properties-common
apt-get install -y apt-transport-https ca-certificates
wget -qO - https://packages.confluent.io/deb/5.1/archive.key | apt-key add -
add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/5.1 stable main"
apt-get update
apt-get install -y librdkafka-dev

