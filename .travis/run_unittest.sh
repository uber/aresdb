#!/usr/bin/env bash
set -ex
# run test-cuda in host mode
make test-cuda -j

# build binary
make ares -j

# run test
ginkgo -r -cover
