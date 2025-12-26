# ============================================================
# src/CMakeLists.txt - Library targets
# ============================================================

# ============================================================
# LMDB (sources compiled into wtree/mongolite)
# ============================================================

set(LMDB_DIR ${CMAKE_SOURCE_DIR}/external/lmdb/libraries/liblmdb)

set(LMDB_SOURCES
    ${LMDB_DIR}/mdb.c
    ${LMDB_DIR}/midl.c
)

# ============================================================
# WTREE Library (includes LMDB)
# ============================================================

set(WTREE_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree.c
    ${LMDB_SOURCES}
)

add_library(wtree STATIC ${WTREE_SOURCES})

target_include_directories(wtree PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${LMDB_DIR}
)

# Export for tests
set(WTREE_SOURCES ${WTREE_SOURCES} PARENT_SCOPE)

# ============================================================
# MONGOLITE Library
# ============================================================

set(MONGOLITE_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/gerror.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree.c
    ${CMAKE_CURRENT_SOURCE_DIR}/wtree2.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_db.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_util.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_schema.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_txn.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_collection.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_insert.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mongolite_find.c
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
