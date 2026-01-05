cmake_minimum_required(VERSION 3.16)
project(mongolite C)

include(ExternalProject)

# ============================================================
# Language & Standards
# ============================================================

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# ============================================================
# Build Type (default to Release)
# ============================================================

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()

set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "RelWithDebInfo")

# ============================================================
# Include Compiler Flags
# ============================================================

include(${CMAKE_SOURCE_DIR}/cmake/CompilerFlags.cmake)

# ============================================================
# Directory Setup
# ============================================================

set(SRC_DIR  ${CMAKE_SOURCE_DIR}/src)
set(TEST_DIR ${CMAKE_SOURCE_DIR}/tests)
set(BIN_DIR  ${CMAKE_BINARY_DIR}/bin)

file(MAKE_DIRECTORY ${BIN_DIR})

include_directories(${SRC_DIR})

# ============================================================
# LMDB (embedded in wtree/mongolite)
# ============================================================

set(LMDB_DIR ${CMAKE_SOURCE_DIR}/external/lmdb/libraries/liblmdb)

include_directories(${LMDB_DIR})

# ============================================================
# LIBBSON (External Project)
# ============================================================

set(MONGOC_DIR ${CMAKE_SOURCE_DIR}/external/mongoc)
set(MONGOC_BUILD_DIR ${CMAKE_BINARY_DIR}/mongoc-build)
set(MONGOC_INSTALL_DIR ${MONGOC_BUILD_DIR}/install)

set(LIBBSON_BUILD_LIB ${MONGOC_INSTALL_DIR}/lib/libbson2.a)
set(LOCAL_LIBBSON_LIB ${CMAKE_BINARY_DIR}/libbson.a)

ExternalProject_Add(mongoc_project
    SOURCE_DIR ${MONGOC_DIR}
    BINARY_DIR ${MONGOC_BUILD_DIR}

    CMAKE_ARGS
        -DENABLE_MONGOC=OFF
        -DENABLE_TESTS=OFF
        -DENABLE_EXAMPLES=OFF
        -DENABLE_STATIC=ON
        -DENABLE_SHARED=OFF
        -DUSE_SYSTEM_LIBBSON=OFF
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${MONGOC_INSTALL_DIR}

    BUILD_COMMAND   ${CMAKE_COMMAND} --build .
    INSTALL_COMMAND ${CMAKE_COMMAND} --build . --target install
)

# Copy static lib to build/
add_custom_command(
    OUTPUT ${LOCAL_LIBBSON_LIB}
    COMMAND ${CMAKE_COMMAND} -E copy
            ${LIBBSON_BUILD_LIB}
            ${LOCAL_LIBBSON_LIB}
    DEPENDS mongoc_project
    COMMENT "Copying static libbson to build/"
)

add_custom_target(libbson ALL DEPENDS ${LOCAL_LIBBSON_LIB})
add_dependencies(libbson mongoc_project)

add_library(bson STATIC IMPORTED)
set_target_properties(bson PROPERTIES
    IMPORTED_LOCATION ${LOCAL_LIBBSON_LIB}
)

include_directories(${MONGOC_INSTALL_DIR}/include/bson-2.3.0)

# ============================================================
# PCRE2 (for bsonmatch)
# ============================================================

set(BSONMATCH_DIR ${CMAKE_SOURCE_DIR}/external/bsonmatch)
set(PCRE2_DIR ${BSONMATCH_DIR}/external/pcre2)

# PCRE2 static config
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC_LIBS ON  CACHE BOOL "" FORCE)
set(PCRE2_BUILD_PCRE2_8  ON  CACHE BOOL "" FORCE)
set(PCRE2_BUILD_PCRE2_16 OFF CACHE BOOL "" FORCE)
set(PCRE2_BUILD_PCRE2_32 OFF CACHE BOOL "" FORCE)
set(PCRE2_SUPPORT_JIT    ON  CACHE BOOL "" FORCE)
set(PCRE2_BUILD_PCRE2GREP OFF CACHE BOOL "" FORCE)
set(PCRE2_BUILD_TESTS     OFF CACHE BOOL "" FORCE)

add_subdirectory(${PCRE2_DIR} ${CMAKE_BINARY_DIR}/pcre2-build)

set(PCRE2_INCLUDE_DIR ${CMAKE_BINARY_DIR}/pcre2-build)

# ============================================================
# BSONMATCH
# ============================================================

# UTHASH (from mongoc)
set(UTHASH_DIR ${CMAKE_SOURCE_DIR}/external/mongoc/src/uthash/uthash-2.3.0)

# bsonmatch sources
set(BSONMATCH_SOURCES
    ${BSONMATCH_DIR}/src/wregex.c
    ${BSONMATCH_DIR}/src/bsoncompare.c
    ${BSONMATCH_DIR}/src/mongoc-matcher.c
    ${BSONMATCH_DIR}/src/mongoc-matcher-op.c
    ${BSONMATCH_DIR}/src/mongoc-matcher-op-geojson.c
    ${BSONMATCH_DIR}/src/mongoc-bson-descendants.c
    ${BSONMATCH_DIR}/src/mongoc-projection.c
    ${BSONMATCH_DIR}/src/mongoc-redaction.c
    ${BSONMATCH_DIR}/src/mongoc-matcher-op-unwind.c
    ${BSONMATCH_DIR}/src/mongoc-matcher-op-conditional.c
    ${BSONMATCH_DIR}/src/mongoc-matcher-op-crypt.c
    ${BSONMATCH_DIR}/src/mongoc-matcher-op-text.c
    ${BSONMATCH_DIR}/src/mongoc-matcher-op-ip.c
    ${BSONMATCH_DIR}/src/mongoc-matcher-op-yara.c
    ${BSONMATCH_DIR}/src/mongoc-matcher-op-modules.c
    ${BSONMATCH_DIR}/src/matcher-module-store.c
    ${BSONMATCH_DIR}/src/matcher-module-between.c
    ${BSONMATCH_DIR}/src/matcher-module-math.c
    ${BSONMATCH_DIR}/src/matcher-module-ether.c
    ${BSONMATCH_DIR}/src/matcher-module-ip.c
    ${BSONMATCH_DIR}/src/matcher-module-disco.c
    ${BSONMATCH_DIR}/src/matcher-module-sample.c
    ${BSONMATCH_DIR}/src/matcher-module-duk.c
)

add_library(bsonmatch STATIC ${BSONMATCH_SOURCES})
add_dependencies(bsonmatch libbson)

target_include_directories(bsonmatch PUBLIC
    ${BSONMATCH_DIR}/src
    ${UTHASH_DIR}
    ${MONGOC_INSTALL_DIR}/include/bson-2.3.0
)
target_include_directories(bsonmatch PRIVATE
    ${PCRE2_INCLUDE_DIR}
    ${PCRE2_DIR}/src
)
target_link_libraries(bsonmatch PUBLIC
    pcre2-8-static
    ${LOCAL_LIBBSON_LIB}
    ${SYSTEM_NETWORK_LIBS}
    m
)
if(WIN32)
    target_link_libraries(bsonmatch PUBLIC bcrypt)
endif()

# ============================================================
# Source Libraries (wtree, mongolite)
# ============================================================

add_subdirectory(src)

# ============================================================
# Tests
# ============================================================

option(BUILD_TESTS "Build tests" ON)

if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# ============================================================
# Benchmarks
# ============================================================

option(BUILD_BENCHMARKS "Build benchmarks" ON)

if(BUILD_BENCHMARKS)
    # Enable C++ for benchmarks (Google Benchmark is C++)
    enable_language(CXX)
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)

    include(FetchContent)

    FetchContent_Declare(
        googlebenchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG        v1.8.3
    )

    # Disable benchmark tests to speed up build
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_WERROR OFF CACHE BOOL "" FORCE)

    # Disable regex support (causes issues on MinGW)
    set(BENCHMARK_USE_BUNDLED_GTEST OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_EXCEPTIONS ON CACHE BOOL "" FORCE)
    set(HAVE_STD_REGEX OFF CACHE BOOL "" FORCE)
    set(HAVE_GNU_POSIX_REGEX OFF CACHE BOOL "" FORCE)
    set(HAVE_POSIX_REGEX OFF CACHE BOOL "" FORCE)

    FetchContent_MakeAvailable(googlebenchmark)

    add_subdirectory(benchmarks)
endif()
