#include <stdlib.h>
#include "rocksdb/c.h"
#include "_cgo_export.h"

void mergeoperator_destructor(void *state) { }

void go_mergeoperator_delete_value(void* id, const char* v, size_t s) { }

rocksdb_mergeoperator_t* native_mergeoperator_create(void* full, void* partial, void *name) {
    return rocksdb_mergeoperator_create(
        NULL,
        mergeoperator_destructor,
        (char* (*)(void*, const char*, size_t, const char*, size_t, const char* const*, const size_t*, int, unsigned char*, size_t*))(full),
        (char* (*)(void*, const char*, size_t, const char* const*, const size_t*, int, unsigned char*, size_t*))(partial),
        NULL,
        (const char *(*)(void*))(name));
}

rocksdb_mergeoperator_t* go_mergeoperator_create(void* state) {
    return rocksdb_mergeoperator_create(
        state,
        mergeoperator_destructor,
        (char* (*)(void*, const char*, size_t, const char*, size_t, const char* const*, const size_t*, int, unsigned char*, size_t*))(gorocksdb_mergeoperator_full_merge),
        (char* (*)(void*, const char*, size_t, const char* const*, const size_t*, int, unsigned char*, size_t*))(gorocksdb_mergeoperator_partial_merge_multi),
        go_mergeoperator_delete_value,
        (const char* (*)(void*))(gorocksdb_mergeoperator_name));
}
