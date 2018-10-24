#!/usr/bin/env bash
go get github.com/axw/gocov/...
go get github.com/AlekSi/gocov-xml
go get github.com/Masterminds/glide
go get -u golang.org/x/lint/golint
go get -u github.com/onsi/ginkgo/ginkgo
go install github.com/Masterminds/glide

# clean build files.
rm -rf ./build