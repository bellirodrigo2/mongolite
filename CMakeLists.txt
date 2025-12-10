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

# =========================
# Directories
# =========================
set(SRC_DIR  ${CMAKE_SOURCE_DIR}/src)
set(TEST_DIR ${CMAKE_SOURCE_DIR}/tests)
set(BIN_DIR  ${CMAKE_BINARY_DIR}/bin)

file(MAKE_DIRECTORY ${BIN_DIR})

include_directories(
    ${SRC_DIR}
    ${CMAKE_SOURCE_DIR}/external/lmdb/libraries/liblmdb
)

# ============================================================
# ========================= LMDB ==============================
# ============================================================

set(LMDB_DIR ${CMAKE_SOURCE_DIR}/external/lmdb/libraries/liblmdb)
set(LMDB_BUILD_LIB ${LMDB_DIR}/liblmdb.a)
set(LOCAL_LMDB_LIB ${CMAKE_BINARY_DIR}/liblmdb.a)

# Detect correct make program
if (WIN32)
    find_program(MAKE_EXE NAMES mingw32-make make)
else()
    find_program(MAKE_EXE NAMES make gmake)
endif()

if (NOT MAKE_EXE)
    message(FATAL_ERROR "No 'make' program found to build LMDB!")
endif()

# Build LMDB
add_custom_command(
    OUTPUT ${LMDB_BUILD_LIB}
    # COMMAND ${MAKE_EXE} -C ${LMDB_DIR} clean
    COMMAND ${MAKE_EXE} -C ${LMDB_DIR} liblmdb.a
    WORKING_DIRECTORY ${LMDB_DIR}
    COMMENT "=== Building ONLY liblmdb.a ==="
)

# Copy LMDB to /build
add_custom_command(
    OUTPUT ${LOCAL_LMDB_LIB}
    COMMAND ${CMAKE_COMMAND} -E copy
            ${LMDB_BUILD_LIB}
            ${LOCAL_LMDB_LIB}
    DEPENDS ${LMDB_BUILD_LIB}
    COMMENT "=== Copying LMDB to build/ ==="
)

add_custom_target(lmdb ALL DEPENDS ${LOCAL_LMDB_LIB})

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
# ========================= SOURCES ======================
# ============================================================

set(GERROR_SRC ${SRC_DIR}/gerror.c)
set(WTREE_SRC  ${SRC_DIR}/wtree.c)

set(LIB_SOURCES
    ${GERROR_SRC}
    ${WTREE_SRC}
)

# ============================================================
# ========================= LIB ===========================
# ============================================================

add_library(wtree STATIC ${LIB_SOURCES})
add_dependencies(wtree lmdb libbson)

target_link_libraries(wtree PRIVATE
    ${LOCAL_LMDB_LIB}
    bson
)

# ============================================================
# ========================= TESTS ============================
# ============================================================

enable_testing()

add_executable(test_gerror
    ${TEST_DIR}/test_gerror.c
    ${GERROR_SRC}
)
add_dependencies(test_gerror lmdb libbson)
target_link_libraries(test_gerror PRIVATE ${LOCAL_LMDB_LIB} bson)
add_test(NAME test_gerror COMMAND test_gerror)

add_executable(test_wtree
    ${TEST_DIR}/test_wtree.c
    ${LIB_SOURCES}
)
add_dependencies(test_wtree lmdb libbson)
target_link_libraries(test_wtree PRIVATE ${LOCAL_LMDB_LIB} bson)
add_test(NAME test_wtree COMMAND test_wtree)

set_target_properties(test_gerror test_wtree
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${BIN_DIR}
)

# =========================
# Target: make test
# =========================
add_custom_target(run-tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    DEPENDS test_gerror test_wtree
)

# =========================
# Clean extra
# =========================
# =========================
# Clean extra (LMDB + LIBBSON + DB de teste)
# =========================
add_custom_target(extra-clean
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/test_wtree_db
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/mongoc-build
    COMMAND ${CMAKE_COMMAND} -E rm -f  ${CMAKE_BINARY_DIR}/libbson.a
    COMMAND ${CMAKE_COMMAND} -E rm -f  ${CMAKE_BINARY_DIR}/liblmdb.a
    COMMENT "Removing test DB, mongoc build, libbson.a and liblmdb.a"
)