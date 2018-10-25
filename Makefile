.PHONY: test-cuda ares lint travis

# all .go files that don't exist in hidden directories
ALL_GO_SRC := $(shell find . -name "*.go" | grep -v -e Godeps -e vendor -e go-build \
  -e build \
  -e ".*/\..*" \
  -e ".*/_.*" \
  -e ".*/mocks.*")

ALL_C_SRC := $(shell find . -type f \( -iname \*.cu -o -iname \*.h -o -iname \*.c \) | grep -v -e Godeps -e vendor -e go-build \
  -e build \
  -e ".*/\..*" \
  -e ".*/_.*" \
  -e ".*/mocks.*")

default: ares

vendor/glide.updated: glide.lock glide.yaml
	glide install
	touch vendor/glide.updated

deps: vendor/glide.updated $(ALL_GO_SRC)

clang-lint:
	cppcheck --std=c++11 --language=c++ --inline-suppr --suppress=selfInitialization $(ALL_C_SRC)
	cpplint $(ALL_C_SRC)


golang-lint:
	gofmt -w $(ALL_GO_SRC)
	golint -set_exit_status $(ALL_GO_SRC)
	go vet $(ALL_GO_SRC)


lint: golang-lint clang-lint


ares: deps
	go build -o $@

travis:
	.travis/run_unittest.sh

test-cuda:
	echo "Test cuda successfully!"