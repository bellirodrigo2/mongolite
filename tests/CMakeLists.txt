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
# Mock wtree infrastructure for unit testing
# ============================================================

set(MOCK_WTREE_SOURCES
    ${TEST_DIR}/mock_wtree.c
)

# Mongolite sources (excluding wtree.c - we mock it)
set(MONGOLITE_SOURCES_NO_WTREE
    ${SRC_DIR}/mongolite_db.c
    ${SRC_DIR}/mongolite_collection.c
    ${SRC_DIR}/mongolite_schema.c
    ${SRC_DIR}/mongolite_cursor.c
    ${SRC_DIR}/mongolite_txn.c
    ${SRC_DIR}/mongolite_insert.c
    ${SRC_DIR}/mongolite_find.c
    ${SRC_DIR}/mongolite_update.c
    ${SRC_DIR}/mongolite_delete.c
    ${SRC_DIR}/mongolite_util.c
    ${SRC_DIR}/bson_update.c
    ${SRC_DIR}/key_compare.c
    ${SRC_DIR}/compare_numbers.c
    ${SRC_DIR}/gerror.c
)

# Macro for mocked unit tests
macro(add_mocked_unit_test NAME)
    add_executable(${NAME}
        ${TEST_DIR}/${NAME}.c
        ${MOCK_WTREE_SOURCES}
        ${MONGOLITE_SOURCES_NO_WTREE}
        ${ARGN}
    )
    set_target_properties(${NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${BIN_DIR})
    target_link_libraries(${NAME} PRIVATE cmocka bsonmatch m pthread)
    target_include_directories(${NAME} PRIVATE
        ${cmocka_SOURCE_DIR}/include
        ${TEST_INCLUDE_DIRS}
        ${TEST_DIR}
    )
    add_dependencies(${NAME} libbson bsonmatch)
    target_link_libraries(${NAME} PRIVATE ${LOCAL_LIBBSON_LIB} ${SYSTEM_NETWORK_LIBS})
    add_test(NAME ${NAME} COMMAND ${NAME})
endmacro()

# compare_numbers test - standalone, no mocking needed
add_mongolite_test(test_compare_numbers
    ${SRC_DIR}/compare_numbers.c
)
add_dependencies(test_compare_numbers libbson)
target_include_directories(test_compare_numbers PRIVATE ${TEST_INCLUDE_DIRS})
target_link_libraries(test_compare_numbers PRIVATE ${LOCAL_LIBBSON_LIB} ${SYSTEM_NETWORK_LIBS})

# ============================================================
# Mocked unit tests
# ============================================================

add_mocked_unit_test(test_mongolite_txn_mocked)
add_mocked_unit_test(test_mongolite_cursor_mocked)
add_mocked_unit_test(test_mongolite_insert_mocked)

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
add_mongolite_integration_test(test_stress)

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
    test_compare_numbers
    test_mongolite_txn_mocked
    test_mongolite_cursor_mocked
    test_mongolite_insert_mocked
    test_mongolite_db
    test_mongolite_collection
    test_mongolite_insert
    test_mongolite_find
    test_mongolite_integration
    test_mongolite_update
    test_mongolite_delete
    test_stress
)

# Custom target to run all tests
add_custom_target(run-tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    DEPENDS ${ALL_TEST_TARGETS}
    COMMENT "Running all tests..."
)
