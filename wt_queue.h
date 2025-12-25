#ifndef WT_QUEUE_H
#define WT_QUEUE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

ALTERAR CONFORME API DO insert_many

/**
* @file wt_queue.h
* @brief MPSC queue for serializing writes with runtime zero-copy and clean shutdown.
*
* DATA CONTRACT (ZERO-COPY):
*  - Producers submit **pointers to key and pointers to value** (user-allocated).
*  - The queue **never copies key/value bytes**.
*  - Memory ownership remains with the producer until dequeued and released.
*  - After dequeue, the consumer **must call wtq_release()** to free key/value using callbacks.
*
* TRIGGERS:
*  - **Batch flush trigger (`on_batch_flush`)**
*      * Invoked by consumer when a batch-drain/flush condition is met upstream
*        (ex: timer or max batch size).
*      * This does NOT commit data to the queue itself — only signals the app layer
*        to flush in batch (e.g., LMDB commit).
*
*  - **Full capacity trigger (`on_full`)**
*      * Invoked when the queue reaches capacity.
*      * `enqueue()` returns `false` immediately (non-blocking).
*      * Callback must be **fast, non-blocking, no locks, no heavy syscalls**.
*
* SHUTDOWN SAFETY:
*  - `wtq_flush()` stops new submissions and wakes the consumer.
*  - `wtq_drain()` waits until `depth == 0` without spinning.
*  - Then `wtq_destroy()` can be safely called.
*
* CONCURRENCY & LIVENESS:
*  - 1 consumer is always safe.
*  - Multiple producers are safe (MPSC).
*  - Avoids deadlocks by requiring that **upstream DB commits happen outside queue locks**.
*  - Fairness is preserved by recommending **bounded batch sizes and timer-based flush**.
*  - Producers must apply backpressure externally if `false` returns are unacceptable.
*
* ARCHITECTURE PORTABILITY:
*  - Compiles on Linux, BSD, Windows.
*  - Ready for extension to Android, iOS, RISC-V, etc.
*/

/** Callback for freeing key memory after consumer finishes processing. */
typedef void (*wtq_free_fn)(const void *ptr, void *arg);

/** Callback triggered when queue is full. Must be fast and non-blocking. */
typedef void (*wtq_on_full_fn)(void *arg);

/** Callback triggered by consumer when a batch flush condition is met upstream. */
typedef void (*wtq_on_batch_flush_fn)(void *arg);

/**
* @struct wtq
* @brief Opaque queue environment.
*/
typedef struct wtq wtq_t;

/**
* @brief Create a new queue.
*
* @param capacity      Maximum number of pointer pairs in flight (recommended power-of-two if using ck_ring).
* @param free_key      Callback to free key pointers after consumption.
* @param fk_arg        Argument passed to free_key().
* @param free_value    Callback to free value pointers after consumption.
* @param fv_arg        Argument passed to free_value().
* @param on_full       Optional callback invoked when queue is full.
* @param fof_arg       Argument passed to on_full().
* @param on_batch_flush Optional callback invoked by consumer when a batch flush trigger fires upstream.
* @param bfl_arg       Argument passed to on_batch_flush().
*
* @return wtq_t* or NULL on failure.
*/
wtq_t *wtq_create(size_t capacity,
                  wtq_free_fn free_key, void *fk_arg,
                  wtq_free_fn free_value, void *fv_arg,
                  wtq_on_full_fn on_full, void *fof_arg,
                  wtq_on_batch_flush_fn on_batch_flush, void *bfl_arg);
/**
* @brief Destroy the queue after clean shutdown (flush + drain).
*/
void wtq_destroy(wtq_t *q);

/**
* @brief Enqueue a user-allocated key/value pointer pair (runtime zero-copy).
*
* @param q        Queue
* @param key      Pointer to key memory (user-allocated)
* @param key_len  Key length (informational only)
* @param value    Pointer to value memory (user-allocated, may be NULL upstream if allowed)
* @param val_len  Value length (informational only)
*
* @return true if enqueued, false if full or queue was flushed.
*
* @note If queue is full and `on_full` callback is configured, it will be invoked.
*/
bool wtq_enqueue(wtq_t *q,
                 const void *key, size_t key_len,
                 const void *value, size_t val_len);
/**
* @brief Dequeue a single key/value pointer pair (consumer only).
*
* @param q         Queue
* @param out_key   Receives key pointer
* @param out_value Receives value pointer
*
* @return true if dequeued, false if empty
*/
bool wtq_dequeue(wtq_t *q, void **out_key, void **out_value);
 
/**
* @brief Dequeue up to `max` pointer pairs into out arrays (consumer only).
*
* @param q         Queue
* @param out_keys  Output array for key pointers
* @param out_vals  Output array for value pointers
* @param max       Max items to dequeue
*
* @return Number of items dequeued
*/
size_t wtq_dequeue_batch(wtq_t *q, void **out_keys, void **out_vals, size_t max);

/**
* @brief Release a dequeued key/value pair after processing.
*
* @param q     Queue
* @param key   Key pointer to free via callback
* @param value Value pointer to free via callback
*/
void wtq_release(wtq_t *q, void *key, void *value);

/**
* @brief Block efficiently until queue becomes non-empty or flushed.
*
* @param q Queue
* @return true if items are available, false if flushed and empty.
*/
bool wtq_wait_nonempty(wtq_t *q);

/**
* @brief Stop accepting new submissions and wake consumer/waiters.
*/
void wtq_flush(wtq_t *q);

/**
* @brief Block until queue depth becomes 0 without busy spinning.
*/
void wtq_drain(wtq_t *q);

/**
* @brief Return current queue depth (exact accounting).
*/
uint64_t wtq_depth(wtq_t *q);

#ifdef __cplusplus
}
#endif

#endif /* WT_QUEUE_H */