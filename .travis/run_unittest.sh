#!/usr/bin/env bash
set -ex

find lib -type f  -exec touch {} +
find gtest -type f  -exec touch {} +
# run test-cuda in host mode
make test-cuda -j

# build binary
make ares -j

# run test
ginkgo -r

echo "mode: atomic" > coverage.out
for file in $(find . -name "*.coverprofile" ! \( -name "coverage.out"  -o -name "expr.coverprofile" \) ); do \
    cat $file | grep -v "mode: atomic" | awk 's=index($0,"ares")+length("ares") { print "." substr($0, s)}' >> coverage.out ; \
    #rm $file ; \
done
gocov convert coverage.out | gocov-xml > coverage.xml
