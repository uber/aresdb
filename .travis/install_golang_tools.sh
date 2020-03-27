#!/usr/bin/env bash
set -ex
go get github.com/axw/gocov/gocov@v1.0.0
go get github.com/AlekSi/gocov-xml@v0.0.0-20190121064608-3a14fb1c4737
go get -u golang.org/x/lint/golint@v0.0.0-20200302205851-738671d3881b
go get -u github.com/onsi/ginkgo/ginkgo@v1.6.0
go get github.com/modocache/gover@v0.0.0-20171022184752-b58185e213c5
go get github.com/mattn/goveralls@v0.0.5
