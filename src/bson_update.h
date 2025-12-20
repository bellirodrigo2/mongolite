/**
 * bson_update.h - BSON Update Operators
 *
 * Provides pure functions for applying MongoDB-style update operators
 * to BSON documents. All functions return new documents (caller must free).
 *
 * Uses single-pass O(n) document rebuilding for optimal performance.
 *
 * Supported operators:
 * - $set    - Set field values
 * - $unset  - Remove fields
 * - $inc    - Increment numeric values
 * - $push   - Append to array
 * - $pull   - Remove from array
 * - $rename - Rename field
 */

#ifndef BSON_UPDATE_H
#define BSON_UPDATE_H

#include <bson/bson.h>
#include "gerror.h"
#include "macros.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Individual Operator Functions
 *
 * Each function applies a single update operator.
 * Returns a NEW document (caller must bson_destroy).
 * Returns NULL on error (error details in gerror_t).
 * ============================================================ */

/**
 * Apply $set operator - set field values
 *
 * @param doc       Source document
 * @param set_iter  Iterator positioned at the $set value (a document)
 * @param error     Error output (may be NULL)
 * @return          New document with fields set, or NULL on error
 */
MONGOLITE_HOT MONGOLITE_WARN_UNUSED
bson_t* bson_update_apply_set(const bson_t *doc, bson_iter_t *set_iter, gerror_t *error);

/**
 * Apply $unset operator - remove fields
 *
 * @param doc         Source document
 * @param unset_iter  Iterator positioned at the $unset value (a document)
 * @param error       Error output (may be NULL)
 * @return            New document with fields removed, or NULL on error
 */
MONGOLITE_WARN_UNUSED
bson_t* bson_update_apply_unset(const bson_t *doc, bson_iter_t *unset_iter, gerror_t *error);

/**
 * Apply $inc operator - increment numeric values
 *
 * @param doc       Source document
 * @param inc_iter  Iterator positioned at the $inc value (a document)
 * @param error     Error output (may be NULL)
 * @return          New document with fields incremented, or NULL on error
 */
MONGOLITE_HOT MONGOLITE_WARN_UNUSED
bson_t* bson_update_apply_inc(const bson_t *doc, bson_iter_t *inc_iter, gerror_t *error);

/**
 * Apply $push operator - append to array
 *
 * @param doc        Source document
 * @param push_iter  Iterator positioned at the $push value (a document)
 * @param error      Error output (may be NULL)
 * @return           New document with values pushed, or NULL on error
 */
MONGOLITE_WARN_UNUSED
bson_t* bson_update_apply_push(const bson_t *doc, bson_iter_t *push_iter, gerror_t *error);

/**
 * Apply $pull operator - remove from array
 *
 * @param doc        Source document
 * @param pull_iter  Iterator positioned at the $pull value (a document)
 * @param error      Error output (may be NULL)
 * @return           New document with values pulled, or NULL on error
 */
MONGOLITE_WARN_UNUSED
bson_t* bson_update_apply_pull(const bson_t *doc, bson_iter_t *pull_iter, gerror_t *error);

/**
 * Apply $rename operator - rename fields
 *
 * @param doc          Source document
 * @param rename_iter  Iterator positioned at the $rename value (a document)
 * @param error        Error output (may be NULL)
 * @return             New document with fields renamed, or NULL on error
 */
MONGOLITE_WARN_UNUSED
bson_t* bson_update_apply_rename(const bson_t *doc, bson_iter_t *rename_iter, gerror_t *error);

/* ============================================================
 * High-Level Update Function
 * ============================================================ */

/**
 * Apply all update operators from an update document
 *
 * Processes operators in order: $set, $unset, $inc, $push, $pull, $rename
 *
 * @param original  Source document
 * @param update    Update document containing operators
 * @param error     Error output (may be NULL)
 * @return          New updated document, or NULL on error
 */
MONGOLITE_HOT MONGOLITE_WARN_UNUSED
bson_t* bson_update_apply(const bson_t *original, const bson_t *update, gerror_t *error);

/**
 * Check if a document is a valid update specification
 *
 * Returns true if the document contains only operator keys (starting with '$').
 * Returns false if it contains non-operator keys (would be a replacement).
 *
 * @param update  Document to check
 * @return        true if valid update spec, false otherwise
 */
MONGOLITE_PURE
bool bson_update_is_update_spec(const bson_t *update);

#ifdef __cplusplus
}
#endif

#endif /* BSON_UPDATE_H */
