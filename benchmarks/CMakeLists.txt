# ============================================================
# Mongolite Benchmarks
# ============================================================

set(BENCH_DIR ${CMAKE_CURRENT_SOURCE_DIR})

# Common include directories for all benchmarks
set(BENCH_INCLUDE_DIRS
    ${SRC_DIR}
    ${LMDB_DIR}
    ${BSONMATCH_DIR}/src
    ${MONGOC_INSTALL_DIR}/include/bson-2.3.0
    ${BENCH_DIR}
)

# Common libraries for all benchmarks
set(BENCH_LIBS
    mongolite
    benchmark::benchmark
    benchmark::benchmark_main
)

# ------------------------------------------------------------
# Macro to add a benchmark executable
# ------------------------------------------------------------

macro(add_benchmark NAME)
    add_executable(${NAME} ${BENCH_DIR}/${NAME}.cpp)
    add_dependencies(${NAME} mongolite)
    target_include_directories(${NAME} PRIVATE ${BENCH_INCLUDE_DIRS})
    target_link_libraries(${NAME} PRIVATE ${BENCH_LIBS})
    set_target_properties(${NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${BIN_DIR})
endmacro()

# ------------------------------------------------------------
# Benchmark executables
# ------------------------------------------------------------

add_benchmark(bench_insert)
add_benchmark(bench_find)
add_benchmark(bench_update)
add_benchmark(bench_delete)

# ------------------------------------------------------------
# Custom targets to run benchmarks
# ------------------------------------------------------------

add_custom_target(run-benchmarks
    COMMAND ${BIN_DIR}/bench_insert
    COMMAND ${BIN_DIR}/bench_find
    COMMAND ${BIN_DIR}/bench_update
    COMMAND ${BIN_DIR}/bench_delete
    DEPENDS bench_insert bench_find bench_update bench_delete
    COMMENT "Running all benchmarks..."
)

add_custom_target(run-bench-insert
    COMMAND ${BIN_DIR}/bench_insert
    DEPENDS bench_insert
    COMMENT "Running insert benchmarks..."
)

add_custom_target(run-bench-find
    COMMAND ${BIN_DIR}/bench_find
    DEPENDS bench_find
    COMMENT "Running find benchmarks..."
)

add_custom_target(run-bench-update
    COMMAND ${BIN_DIR}/bench_update
    DEPENDS bench_update
    COMMENT "Running update benchmarks..."
)

add_custom_target(run-bench-delete
    COMMAND ${BIN_DIR}/bench_delete
    DEPENDS bench_delete
    COMMENT "Running delete benchmarks..."
)
