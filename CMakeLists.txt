cmake_minimum_required(VERSION 3.14)
project(mongolite VERSION 1.0.0 LANGUAGES C)

# Set C standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Build configuration
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

include(FetchContent)

# Fetch SQLite source code (non-amalgamation)
# This allows access to internal B-tree APIs
FetchContent_Declare(
    sqlite_source
    URL https://www.sqlite.org/2024/sqlite-src-3460100.zip
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/deps/sqlite-src
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

FetchContent_MakeAvailable(sqlite_source)

# Fetch MongoDB C driver source code
FetchContent_Declare(
    mongo_c_driver
    GIT_REPOSITORY https://github.com/mongodb/mongo-c-driver.git
    GIT_TAG master
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/deps/mongo-c-driver
)

FetchContent_MakeAvailable(mongo_c_driver)

# Set MongoDB C driver paths
set(MONGO_C_DRIVER_DIR ${CMAKE_CURRENT_SOURCE_DIR}/deps/mongo-c-driver)
set(LIBBSON_DIR ${MONGO_C_DRIVER_DIR}/src/libbson)

# TODO: Build libbson from source later
# add_subdirectory(${MONGO_C_DRIVER_DIR} mongo-c-driver EXCLUDE_FROM_ALL)

# Use the built SQLite source with all headers generated
set(SQLITE_BUILD_DIR ${CMAKE_CURRENT_SOURCE_DIR}/deps/sqlite-src)

# Check if SQLite is already built, if not, build it
if(NOT EXISTS "${SQLITE_BUILD_DIR}/sqlite3.c")
    message(STATUS "Building SQLite from source...")
    execute_process(
        COMMAND ./configure --enable-debug --disable-shared
        WORKING_DIRECTORY ${SQLITE_BUILD_DIR}
        RESULT_VARIABLE CONFIGURE_RESULT
    )
    if(NOT ${CONFIGURE_RESULT} EQUAL 0)
        message(FATAL_ERROR "SQLite configure failed")
    endif()
    
    execute_process(
        COMMAND make -j4
        WORKING_DIRECTORY ${SQLITE_BUILD_DIR}
        RESULT_VARIABLE MAKE_RESULT
    )
    if(NOT ${MAKE_RESULT} EQUAL 0)
        message(FATAL_ERROR "SQLite build failed")
    endif()
endif()

# Create SQLite static library with internal API access
# Uses the generated amalgamation (sqlite3.c) from the source build
add_library(sqlite3 STATIC
    ${SQLITE_BUILD_DIR}/sqlite3.c
)

# Enable internal API access by disabling API armor and making internals public
target_compile_definitions(sqlite3 PRIVATE
    SQLITE_ENABLE_API_ARMOR=0    # Disable API protection
    SQLITE_DEBUG=1               # Enable debug features
    SQLITE_PRIVATE=              # Make internal functions public
    SQLITE_CORE=1               # Core SQLite functionality
)

# Provide access to all SQLite headers including internal ones
target_include_directories(sqlite3 PUBLIC
    ${SQLITE_BUILD_DIR}          # Generated amalgamation headers
    ${SQLITE_BUILD_DIR}/tsrc     # Processed source files
    ${SQLITE_BUILD_DIR}/src      # Original source headers
)

# Create mongolite library
add_library(mongolite STATIC
    src/mongolite.c
    src/mongolite_query.c
)

# Use the bundled libbson from mongo-c-driver
# This approach leverages the properly configured libbson that comes with mongo-c-driver

# Link mongolite with required libraries
target_link_libraries(mongolite PUBLIC 
    sqlite3
    bson_static  # Use the built libbson from mongo-c-driver
)

# Include directories for mongolite
target_include_directories(mongolite PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${LIBBSON_DIR}/src
)

# Test executables
add_executable(test_sqlite tests/test.c)
target_link_libraries(test_sqlite PRIVATE sqlite3)

add_executable(test_btree tests/test_btree.c)
target_link_libraries(test_btree PRIVATE sqlite3)

add_executable(test_mongolite tests/test_mongolite.c)
target_link_libraries(test_mongolite PRIVATE mongolite sqlite3)

add_executable(test_insert_one tests/test_insert_one.c)
target_link_libraries(test_insert_one PRIVATE mongolite sqlite3 bson_static)

add_executable(test_json_insert tests/test_json_insert.c)
target_link_libraries(test_json_insert PRIVATE mongolite sqlite3 bson_static)

add_executable(test_insert_many tests/test_insert_many.c)
target_link_libraries(test_insert_many PRIVATE mongolite sqlite3 bson_static)

add_executable(test_find tests/test_find.c)
target_link_libraries(test_find PRIVATE mongolite sqlite3 bson_static)

add_executable(test_query_ops tests/test_query_ops.c)
target_link_libraries(test_query_ops PRIVATE mongolite sqlite3 bson_static)

add_executable(test_mongodb_precedence tests/test_mongodb_precedence.c)
target_link_libraries(test_mongodb_precedence PRIVATE mongolite sqlite3 bson_static)

add_executable(test_array_operators tests/test_array_operators.c)
target_link_libraries(test_array_operators PRIVATE mongolite sqlite3 bson_static)

add_executable(test_query_operators_unit tests/test_query_operators_unit.c)
target_link_libraries(test_query_operators_unit PRIVATE mongolite sqlite3 bson_static)

# Enable testing
enable_testing()
add_test(NAME basic_sqlite_test COMMAND test_sqlite)
add_test(NAME btree_test COMMAND test_btree)
add_test(NAME mongolite_test COMMAND test_mongolite)
add_test(NAME insert_one_test COMMAND test_insert_one)
add_test(NAME json_insert_test COMMAND test_json_insert)
add_test(NAME insert_many_test COMMAND test_insert_many)
add_test(NAME find_test COMMAND test_find)
add_test(NAME query_ops_test COMMAND test_query_ops)
add_test(NAME mongodb_precedence_test COMMAND test_mongodb_precedence)
add_test(NAME array_operators_test COMMAND test_array_operators)
add_test(NAME query_operators_unit_test COMMAND test_query_operators_unit)