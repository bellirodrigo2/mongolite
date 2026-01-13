/*
 * mongolite_db.c - Database core operations
 *
 * Handles:
 * - Database open/close
 * - Database info and metadata
 *
 * Note: Other functionality has been moved to separate modules:
 * - mongolite_util.c: Utilities, locks, tree cache
 * - mongolite_txn.c: Transaction management
 */

#include "mongolite_internal.h"
#include "key_compare.h"
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>

#ifdef _WIN32
#include <windows.h>
#include <direct.h>
#define mkdir(path, mode) _mkdir(path)
#else
#include <pthread.h>
#endif

#define MONGOLITE_LIB "mongolite"

/* ============================================================
 * Database Open / Close
 * ============================================================ */

int mongolite_open(const char *filename, mongolite_db_t **db,
                   db_config_t *config, gerror_t *error) {
    if (!filename || !db) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "filename and db pointer are required");
        return MONGOLITE_EINVAL;
    }

    *db = NULL;

    /* Check/create directory */
    struct stat st = {0};
    if (stat(filename, &st) == -1) {
        /* Directory doesn't exist, try to create */
        if (mkdir(filename, 0755) != 0) {
            set_error(error, "system", errno,
                     "Failed to create database directory: %s", filename);
            return MONGOLITE_EIO;
        }
    } else if (!S_ISDIR(st.st_mode)) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL,
                 "Path exists but is not a directory: %s", filename);
        return MONGOLITE_EINVAL;
    }

    /* Allocate database structure */
    mongolite_db_t *new_db = calloc(1, sizeof(mongolite_db_t));
    if (!new_db) {
        set_error(error, "system", MONGOLITE_ENOMEM,
                 "Failed to allocate database structure");
        return MONGOLITE_ENOMEM;
    }

    /* Apply configuration */
    size_t max_bytes = config ? config->max_bytes : 0;
    if (max_bytes == 0) max_bytes = MONGOLITE_DEFAULT_MAPSIZE;

    unsigned int max_dbs = config ? config->max_dbs : 0;
    if (max_dbs == 0) max_dbs = MONGOLITE_DEFAULT_MAX_DBS;

    unsigned int lmdb_flags = config ? config->lmdb_flags : 0;

    /* Schema version for wtree3 extractors */
    uint32_t version = WTREE3_VERSION(1, 0);

    /* Open LMDB environment via wtree3 */
    new_db->wdb = wtree3_db_open(filename, max_bytes, max_dbs, version, lmdb_flags, error);
    if (!new_db->wdb) {
        free(new_db);
        return MONGOLITE_ERROR;
    }

    new_db->path = strdup(filename);
    new_db->max_bytes = max_bytes;
    new_db->max_dbs = max_dbs;
    new_db->version = version;

    /* Register BSON key extractors for indexes */
    /* Flags: 0x00 = non-unique, non-sparse; 0x01 = unique; 0x02 = sparse; 0x03 = unique+sparse */
    int rc = wtree3_db_register_key_extractor(new_db->wdb, version, 0x00,
                                          bson_index_key_extractor, error);
    if (rc != 0) {
        wtree3_db_close(new_db->wdb);
        free(new_db->path);
        free(new_db);
        return rc;
    }

    rc = wtree3_db_register_key_extractor(new_db->wdb, version, 0x01,
                                          bson_index_key_extractor, error);
    if (rc != 0) {
        wtree3_db_close(new_db->wdb);
        free(new_db->path);
        free(new_db);
        return rc;
    }

    rc = wtree3_db_register_key_extractor(new_db->wdb, version, 0x02,
                                          bson_index_key_extractor_sparse, error);
    if (rc != 0) {
        wtree3_db_close(new_db->wdb);
        free(new_db->path);
        free(new_db);
        return rc;
    }

    rc = wtree3_db_register_key_extractor(new_db->wdb, version, 0x03,
                                          bson_index_key_extractor_sparse, error);
    if (rc != 0) {
        wtree3_db_close(new_db->wdb);
        free(new_db->path);
        free(new_db);
        return rc;
    }

    /* Initialize mutex */
    rc = _mongolite_lock_init(new_db);
    if (rc != 0) {
        wtree3_db_close(new_db->wdb);
        free(new_db->path);
        free(new_db);
        set_error(error, MONGOLITE_LIB, rc, "Failed to initialize mutex");
        return rc;
    }

    /* Note: Schema system removed - collections are simply wtree3 trees with "col:" prefix */

    *db = new_db;
    return MONGOLITE_OK;
}

int mongolite_close(mongolite_db_t *db) {
    if (!db) return MONGOLITE_OK;

    /* Abort any pending transaction */
    if (db->in_transaction && db->current_txn) {
        wtree3_txn_abort(db->current_txn);
        db->current_txn = NULL;
        db->in_transaction = false;
    }

    /* Clean up pooled read transaction */
    if (db->read_txn_pool) {
        wtree3_txn_abort(db->read_txn_pool);
        db->read_txn_pool = NULL;
    }

    /* Clear tree cache (closes wtree3 collection trees) */
    _mongolite_tree_cache_clear(db);

    /* Close LMDB environment via wtree3 */
    if (db->wdb) {
        wtree3_db_close(db->wdb);
    }

    /* Free mutex */
    _mongolite_lock_free(db);

    free(db->path);
    free(db);

    return MONGOLITE_OK;
}

/* ============================================================
 * Database Info
 * ============================================================ */

const char* mongolite_db_filename(mongolite_db_t *db) {
    return db ? db->path : NULL;
}

int64_t mongolite_last_insert_rowid(mongolite_db_t *db) {
    return db ? db->last_insert_rowid : 0;
}

int mongolite_changes(mongolite_db_t *db) {
    return db ? db->changes : 0;
}

/* Note: Database metadata functions removed - schema system eliminated */
