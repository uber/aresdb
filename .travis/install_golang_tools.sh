#!/usr/bin/env bash
go get github.com/axw/gocov/...
go get github.com/AlekSi/gocov-xml
go get github.com/Masterminds/glide
go install github.com/Masterminds/glide

pushd .
cd vendor/github.com/onsi/ginkgo/ginkgo
go install .
popd
