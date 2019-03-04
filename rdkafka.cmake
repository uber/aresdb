# Build only the static C library
# Does not build C++ library
# Does not build shared libraries
# Does not run tests
# Does not generate documentation


# Protect multiple-inclusion of this CMake file
if( NOT TARGET rdkafka::rdkafka )

# librdkafka depends on pthread and zlib
find_package(Threads REQUIRED)
find_package(ZLIB    REQUIRED)  # sudo apt install zlib1g-dev

# The librdkafka build is based on tools './configure' and 'make'
include(ExternalProject)
ExternalProject_Add(   rdkafka
#   DEPENDS            Threads::Threads ZLIB::ZLIB
    SOURCE_DIR         ${CMAKE_CURRENT_LIST_DIR}/thirdparty/librdkafka
    BUILD_IN_SOURCE    1
#   UPDATE_COMMAND     echo "Full clean (make clean)" && make clean && rm -f Makefile.config config.cache config.h config.log config.log.old rdkafka.pc
    UPDATE_COMMAND     echo "Full clean (make distclean)" && make distclean
    CONFIGURE_COMMAND  ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/librdkafka/configure
                       #--prefix=${CMAKE_BINARY_DIR}
                       --prefix=${CMAKE_CURRENT_SOURCE_DIR}
                       --cc=${CMAKE_C_COMPILER}
                       --cxx=${CMAKE_CXX_COMPILER}
#                      --arch=${MARCH}
                       --CFLAGS=${CMAKE_C_FLAGS_${CMAKE_BUILD_TYPE}}      # TODO(olibre): Retrieve correct flags set by
                       --CXXFLAGS=${CMAKE_CXX_FLAGS_${CMAKE_BUILD_TYPE}}  # add_compile_options() and add_definitions()
                       --LDFLAGS=${CMAKE_STATIC_LINKER_FLAGS}
                       --ARFLAGS=${CMAKE_STATIC_LINKER_FLAGS}
                       --enable-static
    BUILD_COMMAND      echo "Build only librdkafka.a (make librdkafka.a -j4) => No librdkafka++ No shared library No check" &&
                       #make -C src librdkafka.a -j4
                       make
    INSTALL_COMMAND    echo "Install only librdkafka.a" &&
                       make install
    BUILD_BYPRODUCTS   ${CMAKE_BINARY_DIR}/lib/librdkafka.a
)

# Target 'rdkafka::rdkafka' to define: lib-location, include-dir and dependencies
add_library(           rdkafka::rdkafka STATIC IMPORTED GLOBAL )
add_dependencies(      rdkafka::rdkafka rdkafka )
set_target_properties( rdkafka::rdkafka PROPERTIES 
                       IMPORTED_LOCATION             ${CMAKE_CURRENT_SOURCE_DIR}/lib/librdkafka.a
                       INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/librdkafka/src
                       INTERFACE_LINK_LIBRARIES      "Threads::Threads;${ZLIB_LIBRARIES}") #ZLIB::ZLIB

endif()

