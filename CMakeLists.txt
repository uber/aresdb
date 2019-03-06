cmake_minimum_required(VERSION 3.12)
find_package(CUDA REQUIRED)

include_directories(${PROJECT_SOURCE_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY bin)
set(QUERY_MODE $ENV{QUERY_MODE})

# set C_FLAGS and NVCC FLAGS
set(CMAKE_C_FLAGS -fPIC)
set(CMAKE_SHARED_LIBRARY_SUFFIX_C .so)
set(CMAKE_SHARED_LIBRARY_SUFFIX_CXX .so)
set(CUDA_SEPARABLE_COMPILATION ON)
set(CMAKE_CUDA_FLAGS "--cudart shared")
if($ENV{NVCCFLAGS})
    set(NVCCFLAGS $ENV{NVCCFLAGS})
endif()
list(APPEND CUDA_NVCC_FLAGS -lineinfo -I. -std=c++11)
set(GENCODE_FLAGS "-gencode arch=compute_60,code=sm_60" "-gencode arch=compute_60,code=compute_60")

if (NOT QUERY_MODE)
    execute_process(COMMAND which nvidia-smi && nvidia-smi | grep "Driver Version:" RESULT_VARIABLE ret)
    if (NOT ${ret})
        SET(QUERY_MODE DEVICE)
    else ()
        SET(QUERY_MODE HOST)
    endif ()
endif ()

if (QUERY_MODE STREQUAL "DEVICE")
    list(APPEND MARCROS "RUN_ON_DEVICE=1")
    cuda_add_library(mem SHARED memutils/memory.h memutils/memory/cuda_malloc.cu)
else ()
    add_library(mem SHARED memutils/memory.h memutils/memory/malloc.c)
endif ()

# based on DEBUG and QUERY_MODE, transform CUDA_NVCC_FLAGS
if (DEBUG STREQUAL "y")
    set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -g -G --compiler-options='-g -ggdb'")
else ()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")
endif ()

string(REPLACE " " ";" CUDA_XCOMPILER_FLAGS ${CMAKE_C_FLAGS})
list(TRANSFORM CUDA_XCOMPILER_FLAGS PREPEND " -Xcompiler ")
# transform macro to prepend -D prefix
list(TRANSFORM MARCROS PREPEND "-D ")
#list(APPEND CUDA_NVCC_FLAGS ${CUDA_XCOMPILER_FLAGS} ${GENCODE_FLAGS} ${MARCROS})
set(CUDA_NVCC_FLAGS ${CUDA_NVCC_FLAGS};-D RUN_ON_DEVICE=1)
file(MAKE_DIRECTORY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY})
file(MAKE_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})

cuda_add_library(algorithm SHARED
#        query/algorithm.hpp
#        query/algorithm.cu
#        query/binder.hpp
#        query/dimension_transform.cu
#        query/filter.cu
#        query/functor.cu
#        query/geo_intersects.cu
        query/hash_lookup.cu
#        query/hll.cu
#        query/iterator.hpp
#        query/measure_transform.cu
#        query/scratch_space_transform.cu
#        query/sort_reduce.cu
#        query/transform.cu
#        query/transform.hpp
        query/utils.cu
        query/utils.hpp)

################################
# GTest
################################
set(GTEST_ROOT $ENV{GTEST_ROOT})
link_directories(${GTEST_ROOT})
enable_testing()
################################
# Unit Tests
################################
file(GLOB QUERY_UNITTEST_FILES query/*_unittest.cu)
cuda_add_executable(all_unittest ${QUERY_UNITTEST_FILES})
target_include_directories(all_unittest PRIVATE ${GTEST_ROOT}/include ${gtest_SOURCE_DIR})
add_dependencies(all_unittest algorithm)
# Link test executable against gtest & gtest_main
target_link_libraries(all_unittest gtest gtest_main algorithm pthread)
add_test(all_unittest ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/all_unittest)

###############################
# golang
###############################

execute_process(COMMAND bash -c "find . -name '*.go' | grep -v \
  -e vendor -e go-build \
  -e build \
  -e '.*/\\..*' \
  -e '.*/_.*' \
  -e '.*/mocks.*' | tr \"\n\" \" \""
        OUTPUT_VARIABLE ALL_GO_SRC)

execute_process(
        COMMAND bash -c "git diff origin/master HEAD --name-only --diff-filter=AMR | grep -e \".*\\.go\" | tr \"\n\" \" \""
        OUTPUT_VARIABLE CHANGED_GO_SRC
)

execute_process(COMMAND bash -c "find .  -type f \
    \\( -iname \\*.cu -o -iname \\*.h -o -iname \\*.hpp -o -iname \\*.c \\) \
     | grep -v \
  -e vendor -e go-build \
  -e build \
  -e '.*/\\..*' \
  -e '.*/_.*' \
  -e '.*/mocks.*' | tr \"\n\" \" \""
        OUTPUT_VARIABLE ALL_C_SRC)

execute_process(
        COMMAND bash -c "git diff HEAD origin/master --name-only | grep -e \".*\\.\\(cu\\|c\\|h\\|hpp\\)\" | tr \"\n\" \" \""
        OUTPUT_VARIABLE CHANGED_C_SRC
)

add_custom_target(lint-all
        COMMAND ./scripts/clang-lint.sh ${ALL_C_SRC}
        COMMAND ./scripts/golang-lint.sh ${ALL_GO_SRC}
        VERBATIM)

add_custom_target(lint
        COMMAND ./scripts/clang-lint.sh ${CHANGED_C_SRC}
        COMMAND ./scripts/golang-lint.sh ${CHANGED_GO_SRC}
        VERBATIM)

add_custom_target(golang-dep DEPENDS vendor/glide.updated)
add_custom_command(
        OUTPUT vendor/glide.updated
        COMMAND glide install && touch vendor/glide.updated
        DEPENDS glide.lock glide.yaml
        VERBATIM
)

add_custom_target(swagger-gen
        COMMAND swagger generate spec -o api/ui/swagger/swagger.json
        VERBATIM)

add_custom_target(npm-install DEPENDS api/ui/npm.updated)
add_custom_command(
        OUTPUT api/ui/npm.updated
        COMMAND cd api/ui/ && npm install && touch npm.updated
        DEPENDS api/ui/package.json
        VERBATIM
)

add_custom_target(lib DEPENDS algorithm mem)
string(REPLACE " " ";" ALL_GO_SRC_LIST ${ALL_GO_SRC})
add_custom_target(aresd DEPENDS golang-dep lib ${ALL_GO_SRC_LIST}
        COMMAND go build -o bin/aresd
        VERBATIM
        )

add_custom_target(run_server DEPENDS aresd
        COMMAND bash -c 'DYLD_LIBRARY_PATH=$$LIBRARY_PATH ./bin/aresd'
        )

add_custom_target(test-golang DEPENDS aresd
        COMMAND bash -c 'ARES_ENV=test DYLD_LIBRARY_PATH=$$LIBRARY_PATH ginkgo -r'
        )

add_custom_target(travis
        COMMAND bash -c 'ARES_ENV=test .travis/run_unittest.sh'
        )

add_custom_target(
        test-cuda DEPENDS all_unittest
        COMMAND bash -c "DYLD_LIBRARY_PATH=$$LIBRARY_PATH ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/all_unittest --gtest_output=xml:junit.xml"
)
