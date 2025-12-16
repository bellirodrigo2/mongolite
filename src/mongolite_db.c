/*
 * mongolite_db.c - Database core operations
 *
 * Handles:
 * - Database open/close
 * - Database info and metadata
 *
 * Note: Other functionality has been moved to separate modules:
 * - mongolite_util.c: Utilities, locks, tree cache
 * - mongolite_schema.c: Schema management
 * - mongolite_txn.c: Transaction management
 */

#include "mongolite_internal.h"
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

    /* Open LMDB environment */
    new_db->wdb = wtree_db_create(filename, max_bytes, max_dbs, lmdb_flags, error);
    if (!new_db->wdb) {
        free(new_db);
        return MONGOLITE_ERROR;
    }

    new_db->path = strdup(filename);
    new_db->max_bytes = max_bytes;
    new_db->max_dbs = max_dbs;

    /* Initialize mutex */
    int rc = _mongolite_lock_init(new_db);
    if (rc != 0) {
        wtree_db_close(new_db->wdb);
        free(new_db->path);
        free(new_db);
        set_error(error, MONGOLITE_LIB, rc, "Failed to initialize mutex");
        return rc;
    }

    /* Initialize schema tree */
    rc = _mongolite_schema_init(new_db, error);
    if (rc != 0) {
        _mongolite_lock_free(new_db);
        wtree_db_close(new_db->wdb);
        free(new_db->path);
        free(new_db);
        return rc;
    }

    /* Copy user metadata if provided */
    if (config && config->metadata) {
        new_db->db_metadata = bson_copy(config->metadata);
    }

    *db = new_db;
    return MONGOLITE_OK;
}

int mongolite_close(mongolite_db_t *db) {
    if (!db) return MONGOLITE_OK;

    /* Abort any pending transaction */
    if (db->in_transaction && db->current_txn) {
        wtree_txn_abort(db->current_txn);
        db->current_txn = NULL;
        db->in_transaction = false;
    }

    /* Clear tree cache */
    _mongolite_tree_cache_clear(db);

    /* Close schema tree */
    if (db->schema_tree) {
        wtree_tree_close(db->schema_tree);
    }

    /* Close LMDB environment */
    if (db->wdb) {
        wtree_db_close(db->wdb);
    }

    /* Free metadata */
    if (db->db_metadata) {
        bson_destroy(db->db_metadata);
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

/* ============================================================
 * Database Metadata
 * ============================================================ */

const bson_t* mongolite_db_metadata(mongolite_db_t *db) {
    return db ? db->db_metadata : NULL;
}

int mongolite_db_set_metadata(mongolite_db_t *db, const bson_t *metadata, gerror_t *error) {
    if (!db) {
        set_error(error, MONGOLITE_LIB, MONGOLITE_EINVAL, "Database is NULL");
        return MONGOLITE_EINVAL;
    }

    _mongolite_lock(db);

    if (db->db_metadata) {
        bson_destroy(db->db_metadata);
        db->db_metadata = NULL;
    }

    if (metadata) {
        db->db_metadata = bson_copy(metadata);
        if (!db->db_metadata) {
            _mongolite_unlock(db);
            set_error(error, "libbson", MONGOLITE_ENOMEM, "Failed to copy bson metadata");
            return MONGOLITE_ENOMEM;
        }
    }

    _mongolite_unlock(db);
    return MONGOLITE_OK;
}
