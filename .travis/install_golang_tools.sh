#!/usr/bin/env bash
set -ex
go get github.com/axw/gocov/...
go get github.com/AlekSi/gocov-xml
go get -u golang.org/x/lint/golint
go get -u github.com/onsi/ginkgo/ginkgo
go get github.com/modocache/gover
go get github.com/mattn/goveralls
