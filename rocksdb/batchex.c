#include <stdlib.h>
#include "rocksdb/c.h"

char *blevex_rocksdb_execute_direct_batch(
    rocksdb_t* db,
    const unsigned char writeoptions_sync,
    const unsigned char writeoptions_disable_WAL,
    const int num_sets,
    const char* const* set_keys,
    const size_t* set_keys_sizes,
    const char* const* set_vals,
    const size_t* set_vals_sizes,
    int num_deletes,
    const char* const* delete_keys,
    const size_t* delete_keys_sizes,
    int num_merges,
    const char* const* merge_keys,
    const size_t* merge_keys_sizes,
    const char* const* merge_vals,
    const size_t* merge_vals_sizes) {
    rocksdb_writebatch_t* b = rocksdb_writebatch_create();

    if (num_sets > 0) {
        rocksdb_writebatch_putv(b,
            num_sets, set_keys, set_keys_sizes,
            num_sets, set_vals, set_vals_sizes);
    }
    if (num_deletes > 0) {
        rocksdb_writebatch_deletev(b,
            num_deletes, delete_keys, delete_keys_sizes);
    }
    if (num_merges > 0) {
        rocksdb_writebatch_mergev(b,
            num_merges, merge_keys, merge_keys_sizes,
            num_merges, merge_vals, merge_vals_sizes);
    }

    char *errMsg = NULL;

    rocksdb_writeoptions_t *options = rocksdb_writeoptions_create();

    rocksdb_writeoptions_set_sync(options, writeoptions_sync);
    rocksdb_writeoptions_disable_WAL(options, writeoptions_disable_WAL);

    rocksdb_write(db, options, b, &errMsg);

    rocksdb_writeoptions_destroy(options);

    rocksdb_writebatch_destroy(b);

    return errMsg;
}

void blevex_rocksdb_alloc_direct_batch(size_t totalBytes, size_t n, void **out) {
    out[0] = malloc(totalBytes);
    out[1] = malloc(n * sizeof(char *));
    out[2] = malloc(n * sizeof(size_t));
}

void blevex_rocksdb_free_direct_batch(void **bufs) {
    free(bufs[0]);
    free(bufs[1]);
    free(bufs[2]);
}
