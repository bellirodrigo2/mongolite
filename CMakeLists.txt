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

# Test executables
add_executable(test_sqlite tests/test.c)
target_link_libraries(test_sqlite PRIVATE sqlite3)

add_executable(test_btree tests/test_btree.c)
target_link_libraries(test_btree PRIVATE sqlite3)

# Enable testing
enable_testing()
add_test(NAME basic_sqlite_test COMMAND test_sqlite)
add_test(NAME btree_test COMMAND test_btree)