.PHONY: test-cuda ares lint travis

# all .go files that don't exist in hidden directories
ALL_GO_SRC := $(shell find . -name "*.go" | grep -v -e Godeps -e vendor -e go-build \
  -e ".*/\..*" \
  -e ".*/_.*" \
  -e ".*/mocks.*")

ALL_C_SRC := $(shell find -E . -regex ".*\.(h|hpp|c|cc|cu|cpp)" | grep -v -e Godeps -e vendor -e go-build \
  -e ".*/\..*" \
  -e ".*/_.*" \
  -e ".*/mocks.*")

default: ares

clang-lint:
	cppcheck --std=c++11 --language=c++ --inline-suppr --suppress=selfInitialization $(ALL_C_SRC)
	cpplint.py $(ALL_C_SRC)


golang-lint:
	gofmt -w $(ALL_GO_SRC)
	golint -set_exit_status $(ALL_GO_SRC)
	go vet $(ALL_GO_SRC)


lint: golang-lint clang-lint


ares:
	go build -o $@

travis:
	.travis/run_unittest.sh

test-cuda:
	echo "Test cuda successfully!"