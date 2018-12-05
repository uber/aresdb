#!/usr/bin/env bash
set -ex
go get github.com/axw/gocov/...
go get github.com/AlekSi/gocov-xml
go get github.com/Masterminds/glide
go get -u golang.org/x/lint/golint
go get -u github.com/onsi/ginkgo/ginkgo
go install github.com/Masterminds/glide
go get github.com/modocache/gover
go get github.com/mattn/goveralls

# clean build files.
rm -rf ./build
