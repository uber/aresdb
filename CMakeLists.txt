cmake_minimum_required(VERSION 3.12)
# This has to be specified before enable_language according to
# https://gitlab.kitware.com/cmake/cmake/issues/17559
set(CMAKE_CUDA_FLAGS "" CACHE STRING "")
if (CMAKE_CUDA_FLAGS)
    list(REMOVE_ITEM CMAKE_CUDA_FLAGS "-cudart static")
endif ()
string(APPEND CMAKE_CUDA_FLAGS -cudart=shared)

project(aresdb LANGUAGES CUDA C CXX)


# Also needs to be fixed after the find_package.
# https://gitlab.kitware.com/cmake/cmake/issues/17559
if (CMAKE_CUDA_HOST_IMPLICIT_LINK_LIBRARIES)
    list(REMOVE_ITEM CMAKE_CUDA_HOST_IMPLICIT_LINK_LIBRARIES "cudart_static")
endif ()
if (CMAKE_CUDA_IMPLICIT_LINK_LIBRARIES)
    list(REMOVE_ITEM CMAKE_CUDA_IMPLICIT_LINK_LIBRARIES "cudart_static")
endif ()

include(ExternalProject)
include_directories(${PROJECT_SOURCE_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY bin)
set(QUERY_MODE $ENV{QUERY_MODE})

# set C_FLAGS and NVCC FLAGS
set(CMAKE_C_FLAGS -fPIC)
set(CMAKE_SHARED_LIBRARY_SUFFIX_C .so)
set(CMAKE_SHARED_LIBRARY_SUFFIX_CXX .so)
set(CMAKE_CUDA_FLAGS "-lineinfo -I. -std=c++11 $ENV{NVCCFLAGS} ${CMAKE_CUDA_FLAGS}")
set(GENCODE_FLAGS "-gencode arch=compute_60,code=sm_60 -gencode arch=compute_60,code=compute_60")
set(CMAKE_CUDA_SEPARABLE_COMPILATION ON)

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
    add_library(mem SHARED memutils/memory.h memutils/memory/cuda_malloc.cu)
else ()
    add_library(mem SHARED memutils/memory.h memutils/memory/malloc.c)
endif ()

# based on DEBUG and QUERY_MODE, transform CMAKE_CUDA_FLAGS
if (DEBUG STREQUAL "y")
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -g -G --compiler-options='-g -ggdb'")
else ()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")
endif ()

string(REPLACE " " ";" CUDA_XCOMPILER_FLAGS ${CMAKE_C_FLAGS})
list(TRANSFORM CUDA_XCOMPILER_FLAGS PREPEND " -Xcompiler ")
string(REPLACE ";" " " CUDA_XCOMPILER_FLAGS ${CUDA_XCOMPILER_FLAGS})
# transform macro to prepend -D prefix
list(TRANSFORM MARCROS PREPEND "-D ")
set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} ${CUDA_XCOMPILER_FLAGS} ${GENCODE_FLAGS} ${MARCROS}")
file(MAKE_DIRECTORY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY})
file(MAKE_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})

add_library(algorithm SHARED
        query/algorithm.hpp
        query/algorithm.cu
        query/binder.hpp
        query/dimension_transform.cu
        query/filter.cu
        query/functor.cu
        query/geo_intersects.cu
        query/hash_lookup.cu
        query/hll.cu
        query/iterator.hpp
        query/measure_transform.cu
        query/scratch_space_transform.cu
        query/sort_reduce.cu
        query/transform.cu
        query/transform.hpp
        query/utils.cu
        query/utils.hpp)

################################
# GTest
################################
set(GTEST_SRC_DIR ${CMAKE_CURRENT_LIST_DIR}/thirdparty/googletest)
set(GTEST_LIB_DIR ${GTEST_SRC_DIR}/lib)
ExternalProject_Add(googletest
        SOURCE_DIR ${GTEST_SRC_DIR}
        BUILD_IN_SOURCE    1
        # Ignore install command to avoid install lib and include
        # files to /usr/local where we might don't have permission to write to.
        INSTALL_COMMAND ""
        )
link_directories(${GTEST_LIB_DIR})
enable_testing()
######################################################
# RMM
# Can only be built in a machine installed with
# cuda driver api.
######################################################
if (QUERY_MODE STREQUAL "DEVICE")
    set(RMM_SRC_DIR ${CMAKE_CURRENT_LIST_DIR}/thirdparty/rmm)
    set(RMM_LIB_DIR ${RMM_SRC_DIR})
    ExternalProject_Add(rmm
            SOURCE_DIR ${RMM_SRC_DIR}
            BUILD_IN_SOURCE    1
            CONFIGURE_COMMAND cmake -DBUILD_TESTS=OFF .
            # librmm.so will be in ${RMM_SRC_DIR} by default.
            BUILD_COMMAND make -j
            BYPRODUCTS ${RMM_LIB_DIR}/librmm.so
            INSTALL_COMMAND ""
            )
    link_directories(${RMM_LIB_DIR})
endif ()
################################
# Unit Tests
################################
file(GLOB QUERY_UNITTEST_FILES query/*_unittest.cu)
add_executable(all_unittest ${QUERY_UNITTEST_FILES} query/unittest_utils.hpp)
target_include_directories(all_unittest PRIVATE ${GTEST_SRC_DIR}/googletest/include)
add_dependencies(all_unittest algorithm googletest)
# Link test executable against gtest & gtest_main
target_link_libraries(all_unittest gtest gtest_main algorithm pthread)
add_test(all_unittest ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/all_unittest)

###############################
# golang
###############################

execute_process(COMMAND bash -c "find . -name '*.go' | grep -v \
  -e vendor -e go-build -e CMakeFiles \
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
  -e vendor -e go-build -e CMakeFiles \
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

###############################
# misc
###############################

add_custom_target(travis
        COMMAND bash -c 'ARES_ENV=test .travis/run_unittest.sh'
        )

add_custom_target(
        test-cuda DEPENDS all_unittest
        COMMAND bash -c "DYLD_LIBRARY_PATH=$$LIBRARY_PATH ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/all_unittest --gtest_output=xml:junit.xml"
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
