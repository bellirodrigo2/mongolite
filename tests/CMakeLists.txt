# ============================================================
# tests/CMakeLists.txt - Test targets with cmocka
# ============================================================

include(FetchContent)

# ============================================================
# Fetch cmocka
# ============================================================

FetchContent_Declare(
    cmocka
    GIT_REPOSITORY https://gitlab.com/cmocka/cmocka.git
    GIT_TAG cmocka-1.1.7
)

set(WITH_EXAMPLES OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(cmocka)

set(TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR})

# Common include directories
set(TEST_INCLUDE_DIRS
    ${SRC_DIR}
    ${LMDB_DIR}
    ${BSONMATCH_DIR}/src
    ${MONGOC_INSTALL_DIR}/include/bson-2.3.0
)

# ============================================================
# Macro to add a test executable with cmocka
# ============================================================

macro(add_mongolite_test NAME)
    add_executable(${NAME} ${TEST_DIR}/${NAME}.c ${ARGN})
    set_target_properties(${NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${BIN_DIR})
    target_link_libraries(${NAME} PRIVATE cmocka)
    target_include_directories(${NAME} PRIVATE ${cmocka_SOURCE_DIR}/include)
    add_test(NAME ${NAME} COMMAND ${NAME})
endmacro()

# ============================================================
# Standalone tests (minimal dependencies)
# ============================================================

# gerror test - only needs gerror.c
add_mongolite_test(test_gerror ${SRC_DIR}/gerror.c)

# wtree tests - needs wtree + gerror
add_mongolite_test(test_wtree ${WTREE_SOURCES} ${SRC_DIR}/gerror.c)
add_mongolite_test(test_wtree_iter ${WTREE_SOURCES} ${SRC_DIR}/gerror.c)

# key_compare test - needs libbson
add_mongolite_test(test_key_compare ${SRC_DIR}/key_compare.c)
add_dependencies(test_key_compare libbson)
target_include_directories(test_key_compare PRIVATE ${TEST_INCLUDE_DIRS})
target_link_libraries(test_key_compare PRIVATE ${LOCAL_LIBBSON_LIB} ${SYSTEM_NETWORK_LIBS})

# ============================================================
# bsonmatch test
# ============================================================

add_mongolite_test(test_bsonmatch)
add_dependencies(test_bsonmatch bsonmatch)
target_link_libraries(test_bsonmatch PRIVATE bsonmatch m)

# ============================================================
# bson_update test (standalone BSON update operators)
# ============================================================

add_mongolite_test(test_bson_update
    ${SRC_DIR}/bson_update.c
    ${SRC_DIR}/key_compare.c
    ${SRC_DIR}/gerror.c
)
add_dependencies(test_bson_update libbson)
target_include_directories(test_bson_update PRIVATE ${TEST_INCLUDE_DIRS})
target_link_libraries(test_bson_update PRIVATE ${LOCAL_LIBBSON_LIB} ${SYSTEM_NETWORK_LIBS})

# ============================================================
# Mongolite integration tests (full library)
# ============================================================

# Macro for mongolite-dependent tests
macro(add_mongolite_integration_test NAME)
    add_mongolite_test(${NAME})
    add_dependencies(${NAME} mongolite)
    target_link_libraries(${NAME} PRIVATE mongolite)
endmacro()

add_mongolite_integration_test(test_mongolite_db)
add_mongolite_integration_test(test_mongolite_collection)
add_mongolite_integration_test(test_mongolite_insert)
add_mongolite_integration_test(test_mongolite_find)
add_mongolite_integration_test(test_mongolite_integration)
add_mongolite_integration_test(test_mongolite_update)
add_mongolite_integration_test(test_mongolite_delete)

# ============================================================
# Debug test (not part of test suite)
# ============================================================

add_executable(test_debug_update ${TEST_DIR}/test_debug_update.c)
add_dependencies(test_debug_update mongolite)
target_link_libraries(test_debug_update PRIVATE mongolite)
set_target_properties(test_debug_update PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${BIN_DIR})

# ============================================================
# Collect all test targets for dependencies
# ============================================================

set(ALL_TEST_TARGETS
    test_gerror
    test_wtree
    test_wtree_iter
    test_key_compare
    test_bsonmatch
    test_bson_update
    test_mongolite_db
    test_mongolite_collection
    test_mongolite_insert
    test_mongolite_find
    test_mongolite_integration
    test_mongolite_update
    test_mongolite_delete
)

# Custom target to run all tests
add_custom_target(run-tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    DEPENDS ${ALL_TEST_TARGETS}
    COMMENT "Running all tests..."
)
