cmake_minimum_required(VERSION 3.16)
project(wtree C)

include(ExternalProject)

# =========================
# Language
# =========================
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# =========================
# Portable Flags
# =========================
if (MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -fPIC)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    if (MSVC)
        add_compile_options(/Zi /Od)
    else()
        add_compile_options(-g -O0)
    endif()
else()
    if (NOT MSVC)
        add_compile_options(-O2)
    endif()
endif()

if (WIN32)
    set(SYSTEM_NETWORK_LIBS ws2_32)
else()
    set(SYSTEM_NETWORK_LIBS)
endif()

if (MINGW)
    add_compile_definitions(__USE_MINGW_ANSI_STDIO=1)
endif()

# =========================
# Directories
# =========================
set(SRC_DIR  ${CMAKE_SOURCE_DIR}/src)
set(TEST_DIR ${CMAKE_SOURCE_DIR}/tests)
set(BIN_DIR  ${CMAKE_BINARY_DIR}/bin)

file(MAKE_DIRECTORY ${BIN_DIR})

include_directories(
    ${SRC_DIR}
)

# ============================================================
# ========================= LMDB (sources for wtree) ==========
# ============================================================

set(LMDB_DIR ${CMAKE_SOURCE_DIR}/external/lmdb/libraries/liblmdb)

# LMDB sources - will be compiled into libwtree.a
set(LMDB_SOURCES
    ${LMDB_DIR}/mdb.c
    ${LMDB_DIR}/midl.c
)

include_directories(
    ${SRC_DIR}
    ${LMDB_DIR}
)


# ============================================================
# ========================= LIBBSON ===========================
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

# Copiar SOMENTE o .a para /build
add_custom_command(
    OUTPUT ${LOCAL_LIBBSON_LIB}
    COMMAND ${CMAKE_COMMAND} -E copy
            ${LIBBSON_BUILD_LIB}
            ${LOCAL_LIBBSON_LIB}
    DEPENDS mongoc_project
    COMMENT "=== Copying ONLY static libbson to build/ ==="
)

add_custom_target(libbson ALL DEPENDS ${LOCAL_LIBBSON_LIB})
add_dependencies(libbson mongoc_project)

add_library(bson STATIC IMPORTED)
set_target_properties(bson PROPERTIES
    IMPORTED_LOCATION ${LOCAL_LIBBSON_LIB}
)

include_directories(
    ${MONGOC_INSTALL_DIR}/include/bson-2.3.0
)

# ============================================================
# ========================= PCRE2 (for bsonmatch) =============
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
# ========================= BSONMATCH =========================
# ============================================================

# UTHASH (from mongoc)
set(UTHASH_DIR ${CMAKE_SOURCE_DIR}/external/mongoc/src/uthash/uthash-2.3.0)

# bsonmatch sources (includes wregex.c - no separate lib)
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
)
if(WIN32)
    target_link_libraries(bsonmatch PUBLIC bcrypt)
endif()

# ============================================================
# ========================= WTREE (includes LMDB) =============
# ============================================================

# wtree sources (includes LMDB - no separate lib)
set(WTREE_SOURCES
    ${SRC_DIR}/wtree.c
    ${LMDB_SOURCES}
)

add_library(wtree STATIC ${WTREE_SOURCES})

# ============================================================
# ========================= TESTS ============================
# ============================================================

enable_testing()

# --- test para gerror

add_executable(test_gerror
    ${TEST_DIR}/test_gerror.c
)
add_test(NAME test_gerror COMMAND test_gerror)

# --- test para wtree (uses WTREE_SOURCES directly)

add_executable(test_wtree
    ${TEST_DIR}/test_wtree.c
    ${WTREE_SOURCES}
)
add_test(NAME test_wtree COMMAND test_wtree)

add_executable(test_wtree_iter
    ${TEST_DIR}/test_wtree_iter.c
    ${WTREE_SOURCES}
)
add_test(NAME test_wtree_iter COMMAND test_wtree_iter)

# --- test para key_compare

add_executable(test_key_compare
    ${TEST_DIR}/test_key_compare.c
    ${SRC_DIR}/key_compare.c
)
add_dependencies(test_key_compare libbson)
target_link_libraries(test_key_compare PRIVATE ${LOCAL_LIBBSON_LIB}
    ${SYSTEM_NETWORK_LIBS})
add_test(NAME test_key_compare COMMAND test_key_compare)

# --- test para bsonmatch

add_executable(test_bsonmatch
    ${TEST_DIR}/test_bsonmatch.c
)
add_dependencies(test_bsonmatch bsonmatch)
target_link_libraries(test_bsonmatch PRIVATE bsonmatch)
add_test(NAME test_bsonmatch COMMAND test_bsonmatch)

set_target_properties(test_gerror test_wtree test_wtree_iter test_key_compare test_bsonmatch
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${BIN_DIR}
)


# =========================
# Target: make test
# =========================

add_custom_target(run-tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    DEPENDS test_gerror test_wtree test_wtree_iter test_key_compare test_bsonmatch
)
