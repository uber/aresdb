#!/usr/bin/env bash
set -ex
go get github.com/axw/gocov/gocov@v1.0.0
go get github.com/AlekSi/gocov-xml
go get -u golang.org/x/lint/golint
go get -u github.com/onsi/ginkgo/ginkgo@v1.6.0
go get github.com/modocache/gover
go get github.com/mattn/goveralls@v0.0.5
