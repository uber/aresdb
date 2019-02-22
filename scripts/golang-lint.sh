#!/usr/bin/env bash
GO_SRC=$1
if [[ ! -z ${GO_SRC} ]]
then
    gofmt -w ${GO_SRC}
	golint -set_exit_status ${GO_SRC}
	go vet -unsafeptr=false ./...
fi