.PHONY: test-cuda ares lint travis deps swagger-gen npm-install clean clean-cuda-test test

# all .go files that don't exist in hidden directories
ALL_GO_SRC := $(shell find . -name "*.go" | grep -v -e Godeps -e vendor -e go-build \
  -e build \
  -e ".*/\..*" \
  -e ".*/_.*" \
  -e ".*/mocks.*")

ALL_C_SRC := $(shell find . -type f \( -iname \*.cu -o -iname \*.h \) | grep -v -e Godeps -e vendor -e go-build \
  -e build \
  -e ".*/\..*" \
  -e ".*/_.*" \
  -e ".*/mocks.*")

CUDA_DRIVER_ENABLED := $(shell which nvidia-smi && nvidia-smi | grep "Driver Version:")

QUERY_MODE := ${QUERY_MODE}

default: ares

vendor/glide.updated: glide.lock glide.yaml
	glide install
	touch vendor/glide.updated

deps: vendor/glide.updated $(ALL_GO_SRC)

pwd := $(shell pwd)

swagger-gen:
	swagger generate spec -o api/ui/swagger/swagger.json

npm-install:
	cd api/ui/ && npm install

ifndef QUERY_MODE
    QUERY_MODE := $(if $(CUDA_DRIVER_ENABLED),DEVICE,HOST)
endif

# specify dependencies for memory allocation library based on whether cuda driver is available.
ifeq "$(QUERY_MODE)" "DEVICE"
lib/libmem.so:
	@make cuda_malloc
else
lib/libmem.so:
	@make malloc
endif

libs: lib/libmem.so

clang-lint:
	cppcheck --std=c++11 --language=c++ --inline-suppr --suppress=selfInitialization $(ALL_C_SRC)
	cpplint $(ALL_C_SRC)


golang-lint:
	gofmt -w $(ALL_GO_SRC)
	golint -set_exit_status $(ALL_GO_SRC)
	go vet


lint: deps golang-lint clang-lint


ares: deps libs
	go build -o $@

run: ares
	./ares

clean:
	-rm -rf lib

clean-cuda-test:
	-rm -rf gtest/*

test: ares
	bash -c 'DYLD_LIBRARY_PATH=$$LIBRARY_PATH ginkgo -r'

travis:
	ARES_ENV=test .travis/run_unittest.sh

test-cuda :
	@mkdir -p $(GTEST_OUT_DIR)
	$(MAKE) $(GTEST_OUT_DIR)/all_unittest
	$(GTEST_OUT_DIR)/all_unittest --gtest_output=xml:junit.xml

#####################################
# Cuda library build related targets#
#####################################
NVCC = nvcc
CCFLAGS     := -fPIC
LDFLAGS     := -lstdc++
GENCODE_FLAGS := -gencode arch=compute_60,code=sm_60 -gencode arch=compute_60,code=compute_60

ALL_CCFLAGS := -m64 -lineinfo -I. -std=c++11 $(NVCCFLAGS)

ifeq ($(DEBUG), y)
ALL_CCFLAGS += -g -G --compiler-options='-g -ggdb'
else
CCFLAGS += -O3
endif
ALL_CCFLAGS += $(addprefix -Xcompiler ,$(CCFLAGS))

ALL_LDFLAGS := $(ALL_CCFLAGS)
ALL_LDFLAGS += $(addprefix -Xlinker ,$(LDFLAGS))

ifeq ($(QUERY_MODE), DEVICE)
MARCROS += RUN_ON_DEVICE=1
endif

MARCROS := $(addprefix -D ,$(MARCROS))

ALGO_SOURCE_FILES=$(notdir $(filter-out $(wildcard query/*_unittest.cu),$(wildcard query/*.cu)))

ALGO_OBJECT_DIR=lib/algorithm

ALGO_OBJECT_FILES = $(patsubst %.cu,$(ALGO_OBJECT_DIR)/%.o,$(ALGO_SOURCE_FILES))

CXX_SRC_DIR = .
GTEST_OUT_DIR = gtest

lib/algorithm:
	mkdir -p lib/algorithm

lib: lib/algorithm
	mkdir -p lib

# header files dependencies.
query/algorithm.h: lib query/utils.h query/iterator.h query/functor.h query/time_series_aggregate.h

query/transform.h: query/algorithm.h

# cuda source files dependencies.
query/algorithm.cu: query/algorithm.h

query/transform.cu: query/transform.h

query/scracth_space_transform.cu: query/transform.h

query/measure_transform.cu: query/transform.h

query/dimension_transform.cu: query/transform.h

query/hash_lookup.cu: query/transform.h

query/filter.cu: query/transform.h

query/sort_reduce.cu: query/algorithm.h

query/hll.cu: query/algorithm.h

query/geo_intersects.cu: query/algorithm.h

query/utils.cu: query/utils.h

query/functor.cu: query/functor.h

lib/algorithm/%.o: query/%.cu
	$(NVCC) $(ALL_CCFLAGS) $(GENCODE_FLAGS) $(MARCROS) -dc -o $@ -c $^

lib/libalgorithm.so: $(ALGO_OBJECT_FILES)
	$(NVCC) $(ALL_CCFLAGS) $(GENCODE_FLAGS) -o $@ $^ -shared -cudart=shared

malloc: lib
	gcc $(CCFLAGS) -o lib/malloc.o -c memutils/memory/malloc.c
	gcc -shared -o lib/libmem.so lib/malloc.o

cuda_malloc: lib
	$(NVCC) $(ALL_CCFLAGS) $(GENCODE_FLAGS) -o lib/cuda_malloc.o -c memutils/memory/cuda_malloc.cu
	$(NVCC) $(ALL_CCFLAGS) $(GENCODE_FLAGS) -o lib/libmem.so lib/cuda_malloc.o -shared -cudart=shared

#####################################
# Cuda library test related targets#
#####################################

# Flags passed to the preprocessor.
# Set Google Test's header directory as a system directory, such that
# the compiler doesn't generate warnings in Google Test headers.
CPPFLAGS += -isystem $(GTEST_DIR)/include -I. -L./lib

CPPFLAGS += $(MARCROS)

# Flags passed to the C++ compiler.
CXXFLAGS += -g -std=c++11

# All tests produced by this Makefile. Remember to add new tests you
# created to the list.
CUDA_TESTS = algorithm_unittest functor_unittest  iterator_unittest

GTEST_DIR = ${GTEST_ROOT}
# All Google Test headers.  Usually you shouldn't change this
# definition.
GTEST_HEADERS = $(GTEST_DIR)/include/gtest/*.h \
                $(GTEST_DIR)/include/gtest/internal/*.h

# Builds gtest.a and gtest_main.a.

# Usually you shouldn't tweak such internal variables, indicated by a
# trailing _.
GTEST_SRCS_ = $(GTEST_DIR)/src/*.cc $(GTEST_DIR)/src/*.h $(GTEST_HEADERS)

$(GTEST_OUT_DIR)/gtest-all.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest-all.cc -o $@

$(GTEST_OUT_DIR)/gtest_main.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest_main.cc  -o $@

$(GTEST_OUT_DIR)/gtest.a : $(GTEST_OUT_DIR)/gtest-all.o
	$(AR) $(ARFLAGS) $@ $^

$(GTEST_OUT_DIR)/gtest_main.a : $(GTEST_OUT_DIR)/gtest-all.o $(GTEST_OUT_DIR)/gtest_main.o
	$(AR) $(ARFLAGS) $@ $^

$(CXX_SRC_DIR)/query/algorithm_unittest.cu : lib/libalgorithm.so $(CXX_SRC_DIR)/query/unittest_utils.h $(GTEST_HEADERS)

$(CXX_SRC_DIR)/query/iterator_unittest.cu : lib/libalgorithm.so $(CXX_SRC_DIR)/query/iterator.h $(CXX_SRC_DIR)/query/unittest_utils.h $(CXX_SRC_DIR)/query/utils.h $(GTEST_HEADERS)

$(CXX_SRC_DIR)/query/functor_unittest.cu : lib/libalgorithm.so $(CXX_SRC_DIR)/query/iterator.h $(CXX_SRC_DIR)/query/unittest_utils.h $(CXX_SRC_DIR)/query/utils.h  $(CXX_SRC_DIR)/query/functor.h $(GTEST_HEADERS)

$(GTEST_OUT_DIR)/%_unittest.o: $(CXX_SRC_DIR)/query/%_unittest.cu
	$(NVCC) $(CPPFLAGS) $(CXXFLAGS) $(NVCCFLAGS) -c $^ -o $@

$(GTEST_OUT_DIR)/all_unittest : $(patsubst %,$(GTEST_OUT_DIR)/%.o,$(CUDA_TESTS)) $(GTEST_OUT_DIR)/gtest_main.a
	ls -r
	$(NVCC) $(CPPFLAGS) $(CXXFLAGS) $(NVCCFLAGS) $^ -o $@  -lpthread -lalgorithm
