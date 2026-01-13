# ============================================================
# src/CMakeLists.txt - Library targets
# ============================================================

# ============================================================
# LMDB (sources compiled into wtree3/mongolite)
# ============================================================

set(LMDB_DIR ${CMAKE_SOURCE_DIR}/external/lmdb/libraries/liblmdb)

set(LMDB_SOURCES
    ${LMDB_DIR}/mdb.c
    ${LMDB_DIR}/midl.c
)

# ============================================================
# WTREE3 Library (unified storage layer - modular version)
# ============================================================

set(WTREE3_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_crud.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_index.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_index_persist.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_iterator.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_scan.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_tree.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_memopt.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_extractor_registry.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wvector.c
    ${CMAKE_CURRENT_SOURCE_DIR}/gerror.c
    ${LMDB_SOURCES}
)

add_library(wtree3 STATIC ${WTREE3_SOURCES})

target_include_directories(wtree3 PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3
    ${LMDB_DIR}
)

# Export for tests
set(WTREE3_SOURCES ${WTREE3_SOURCES} PARENT_SCOPE)

# ============================================================
# MONGOLITE Library
# ============================================================

set(MONGOLITE_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/gerror.c
    # wtree3 modular sources
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_crud.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_index.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_index_persist.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_iterator.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_scan.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_tree.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_memopt.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wtree3_extractor_registry.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree3/wvector.c
    # mongolite sources
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_db.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_util.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_txn.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_collection.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_insert.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_find.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_query_index.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_cursor.c
    ${CMAKE_CURRENT_SOURCE_DIR}/bson_update.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_update.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_delete.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_index.c
    ${CMAKE_CURRENT_SOURCE_DIR}/key_compare.c
    ${LMDB_SOURCES}
)

add_library(mongolite STATIC ${MONGOLITE_SOURCES})
add_dependencies(mongolite libbson bsonmatch)

target_include_directories(mongolite PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${LMDB_DIR}
    ${BSONMATCH_DIR}/src
    ${MONGOC_INSTALL_DIR}/include/bson-2.3.0
)

target_link_libraries(mongolite PUBLIC
    bsonmatch
    ${LOCAL_LIBBSON_LIB}
    ${SYSTEM_NETWORK_LIBS}
)

# Export sources for tests that need them directly
set(MONGOLITE_SOURCES ${MONGOLITE_SOURCES} PARENT_SCOPE)
