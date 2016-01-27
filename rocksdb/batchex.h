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
    const size_t* merge_vals_sizes);

void blevex_rocksdb_alloc_direct_batch(size_t totalBytes, size_t n, void **out);

void blevex_rocksdb_free_direct_batch(void **bufs);
