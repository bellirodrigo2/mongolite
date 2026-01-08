cmake_minimum_required(VERSION 3.15)
project(bsonmatch C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Required for MinGW to use correct printf format specifiers (PRIu64, etc.)
if(MINGW)
    add_compile_definitions(__USE_MINGW_ANSI_STDIO=1)
endif()

include(ExternalProject)

# ---------------------------------------------------------
# UTHASH (header-only)
# ---------------------------------------------------------
set(UTHASH_DIR "${CMAKE_SOURCE_DIR}/external/mongoc/src/uthash/uthash-2.3.0")

if(EXISTS "${UTHASH_DIR}/uthash.h")
    message(STATUS "Using uthash from: ${UTHASH_DIR}")
else()
    message(WARNING "uthash not found in external/, assuming system include path")
endif()

# ---------------------------------------------------------
#  PCRE2 — build STATIC somente 8 bits
# ---------------------------------------------------------
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC_LIBS ON  CACHE BOOL "" FORCE)

set(PCRE2_BUILD_PCRE2_8  ON  CACHE BOOL "" FORCE)
set(PCRE2_BUILD_PCRE2_16 OFF CACHE BOOL "" FORCE)
set(PCRE2_BUILD_PCRE2_32 OFF CACHE BOOL "" FORCE)

set(PCRE2_SUPPORT_JIT    ON  CACHE BOOL "" FORCE)

# desabilita coisas desnecessárias
set(PCRE2_BUILD_PCRE2GREP OFF CACHE BOOL "" FORCE)
set(PCRE2_BUILD_TESTS     OFF CACHE BOOL "" FORCE)

add_subdirectory(external/pcre2)

# O pcre2.h gerado fica no binary dir do subprojeto
set(PCRE2_INCLUDE_DIR "${CMAKE_BINARY_DIR}/external/pcre2")

# ---------------------------------------------------------
# Wrapper wregex
# ---------------------------------------------------------
add_library(wregex STATIC
    src/wregex.c
    src/wregex.h
)

target_include_directories(wregex PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${UTHASH_DIR}
)

target_include_directories(wregex PRIVATE
    ${PCRE2_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}/external/pcre2/src  # pcre2.h.in headers
)

target_link_libraries(wregex PUBLIC
    pcre2-8-static
)

# ---------------------------------------------------------
# LIBBSON - 3 modos de uso:
# ---------------------------------------------------------
# Modo 1 (testes): Auto-compilar do submodule
#   cmake -DBUILD_LIBBSON_LOCAL=ON ..
#
# Modo 2 (externo): Fornecer path para libbson já compilado
#   cmake -DLIBBSON_LIBRARY=/path/to/libbson.a -DLIBBSON_INCLUDE=/path/to/include ..
#
# Modo 3 (padrão): Sem libbson - compila apenas wregex
#   cmake ..

option(BUILD_LIBBSON_LOCAL "Auto-compile libbson from external/mongoc submodule" OFF)
set(LIBBSON_LIBRARY "" CACHE FILEPATH "Path to libbson static library (.a)")
set(LIBBSON_INCLUDE "" CACHE PATH "Path to libbson headers (folder containing bson/)")

# Modo 1: Auto-compilar libbson
if(BUILD_LIBBSON_LOCAL)
    message(STATUS "Building libbson from external/mongoc...")

    set(MONGOC_DIR ${CMAKE_SOURCE_DIR}/external/mongoc)
    set(MONGOC_BUILD_DIR ${CMAKE_BINARY_DIR}/mongoc-build)
    set(MONGOC_INSTALL_DIR ${MONGOC_BUILD_DIR}/install)

    ExternalProject_Add(mongoc_project
        SOURCE_DIR ${MONGOC_DIR}
        BINARY_DIR ${MONGOC_BUILD_DIR}

        CMAKE_ARGS
            -G "MinGW Makefiles"
            -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
            -DENABLE_MONGOC=OFF
            -DENABLE_TESTS=OFF
            -DENABLE_EXAMPLES=OFF
            -DENABLE_STATIC=ON
            -DENABLE_SHARED=OFF
            -DENABLE_EXTRA_ALIGNMENT=OFF
            -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}

        BUILD_COMMAND ${CMAKE_COMMAND} --build . --target bson_static
        INSTALL_COMMAND ""  # Skip install - use files directly from build
    )

    # Paths para libbson compilado (direto do build, sem install)
    set(LIBBSON_LIBRARY ${MONGOC_BUILD_DIR}/src/libbson/libbson2.a)
    # Headers: source dir has bson/ folder, build dir has generated bson/config.h, bson/version.h
    set(LIBBSON_INCLUDE_SRC ${MONGOC_DIR}/src/libbson/src)
    set(LIBBSON_INCLUDE_BUILD ${MONGOC_BUILD_DIR}/src/libbson/src)

    add_library(bson STATIC IMPORTED)
    set_target_properties(bson PROPERTIES
        IMPORTED_LOCATION ${LIBBSON_LIBRARY}
    )
    add_dependencies(bson mongoc_project)

    set(BSONMATCH_BUILD_LIB ON)

# Modo 2: Usar libbson externo fornecido
elseif(LIBBSON_LIBRARY AND LIBBSON_INCLUDE)
    message(STATUS "Using external libbson: ${LIBBSON_LIBRARY}")
    message(STATUS "Using libbson headers: ${LIBBSON_INCLUDE}")

    add_library(bson STATIC IMPORTED)
    set_target_properties(bson PROPERTIES
        IMPORTED_LOCATION ${LIBBSON_LIBRARY}
    )

    set(BSONMATCH_BUILD_LIB ON)

# Modo 3: Sem libbson
else()
    message(STATUS "LIBBSON not configured - building wregex only")
    set(BSONMATCH_BUILD_LIB OFF)
endif()

# ---------------------------------------------------------
# BSONMATCH Library (only if libbson available)
# ---------------------------------------------------------
if(BSONMATCH_BUILD_LIB)
    set(BSONMATCH_SOURCES
        src/bsoncompare.c
        src/mongoc-matcher.c
        src/mongoc-matcher-op.c
        src/mongoc-matcher-op-geojson.c
        src/mongoc-bson-descendants.c
        src/mongoc-projection.c
        src/mongoc-redaction.c
        src/mongoc-matcher-op-unwind.c
        src/mongoc-matcher-op-conditional.c
        src/mongoc-matcher-op-crypt.c
        src/mongoc-matcher-op-text.c
        src/mongoc-matcher-op-ip.c
        src/mongoc-matcher-op-yara.c
        src/mongoc-matcher-op-modules.c
        src/matcher-module-store.c
        src/matcher-module-between.c
        src/matcher-module-math.c
        src/matcher-module-ether.c
        src/matcher-module-ip.c
        src/matcher-module-disco.c
        src/matcher-module-sample.c
        src/matcher-module-duk.c
    )

    add_library(bsonmatch STATIC ${BSONMATCH_SOURCES})

    target_include_directories(bsonmatch PUBLIC
        ${CMAKE_SOURCE_DIR}/src
        ${UTHASH_DIR}
    )

    # libbson include paths differ depending on build mode
    if(BUILD_LIBBSON_LOCAL)
        target_include_directories(bsonmatch PUBLIC
            ${LIBBSON_INCLUDE_SRC}      # For <bson/bson.h>
            ${LIBBSON_INCLUDE_BUILD}    # For generated config.h, version.h
        )
        add_dependencies(bsonmatch mongoc_project)
    else()
        target_include_directories(bsonmatch PUBLIC ${LIBBSON_INCLUDE})
    endif()

    target_include_directories(bsonmatch PRIVATE
        ${PCRE2_INCLUDE_DIR}
        ${CMAKE_SOURCE_DIR}/external/pcre2/src
    )

    target_link_libraries(bsonmatch PUBLIC
        wregex
        bson
    )

    # Platform-specific libraries
    if(UNIX)
        target_link_libraries(bsonmatch PUBLIC m)
    endif()
    if(WIN32)
        # Windows: ws2_32 needed for gethostname, bcrypt for random
        target_link_libraries(bsonmatch PUBLIC ws2_32 bcrypt)
    endif()
endif()


# ---------------------------------------------------------
# Tests
# ---------------------------------------------------------
enable_testing()

# wregex test (always available)
add_executable(test_wregex tests/test_wregex.c)
target_link_libraries(test_wregex PRIVATE wregex)
add_test(NAME test_wregex COMMAND test_wregex)

# bsonmatch tests (only if lib is built)
if(BSONMATCH_BUILD_LIB)
    function(bsonmatch_add_test test_name source_file)
        add_executable(${test_name} ${source_file})
        target_link_libraries(${test_name} PRIVATE bsonmatch)
        add_test(NAME ${test_name} COMMAND ${test_name})
    endfunction()

    # Core tests
    bsonmatch_add_test(test_simple tests/simple.c)
    bsonmatch_add_test(test_bcon_samples tests/bcon_samples.c)
    bsonmatch_add_test(test_compare_equal tests/bsoncompare_simple.c)
    bsonmatch_add_test(test_mongodbtests tests/matchertest.c)
    bsonmatch_add_test(test_regex tests/bsoncompare_regex.c)
    bsonmatch_add_test(test_regex_extended tests/bsoncompare_regex_extended.c)
    bsonmatch_add_test(test_leak tests/bsoncompare_leak_test.c)
    bsonmatch_add_test(test_gte tests/bsoncompare_gte_lists.c)
    bsonmatch_add_test(test_size tests/bsoncompare_opcode_size.c)
    bsonmatch_add_test(test_near tests/bsoncompare_near.c)
    bsonmatch_add_test(test_datetime tests/bsoncompare_datetime.c)
    bsonmatch_add_test(test_oid tests/bsoncompare_oid.c)
    bsonmatch_add_test(test_geonear tests/bsoncompare_geonear.c)
    bsonmatch_add_test(test_box tests/bsoncompare_box.c)
    bsonmatch_add_test(test_descendants tests/bson_descendants.c)
    bsonmatch_add_test(test_bigdoc tests/bigdoctest.c)
    bsonmatch_add_test(test_inset tests/bsoncompare_inset.c)
    bsonmatch_add_test(test_and_bail tests/bsoncompare_and_bailout.c)
    bsonmatch_add_test(test_json_decode tests/bsoncompare_json_input.c)
    bsonmatch_add_test(test_typecheck tests/bsoncompare-typecheck.c)
    bsonmatch_add_test(test_exists tests/bsoncompare_exists.c)
    bsonmatch_add_test(test_strlen tests/bsoncompare_strlen.c)
    bsonmatch_add_test(test_geospace tests/bsoncompare_geospace.c)
endif()

# ---------------------------------------------------------
# Convenience target
# ---------------------------------------------------------
add_custom_target(run-tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    DEPENDS test_wregex
)