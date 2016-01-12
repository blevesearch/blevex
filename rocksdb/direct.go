//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rocksdb

/*
#include <stdio.h>
#include <stdlib.h>
#include "rocksdb/c.h"

char *blevex_rocksdb_execute_direct_batch(
    rocksdb_t* db,
    const int num_sets,
    const char* const* set_keys_list,
    const size_t* set_keys_list_sizes,
    const char* const* set_values_list,
    const size_t* set_values_list_sizes,
    int num_merges,
    const char* const* merge_keys_list,
    const size_t* merge_keys_list_sizes,
    const char* const* merge_values_list,
    const size_t* merge_values_list_sizes,
    int num_deletes,
    const char* const* delete_keys_list,
    const size_t* delete_keys_list_sizes) {
    rocksdb_writebatch_t* b = rocksdb_writebatch_create();

    rocksdb_writebatch_putv(b,
        num_sets, set_keys_list, set_keys_list_sizes,
        num_sets, set_values_list, set_values_list_sizes);
    rocksdb_writebatch_mergev(b,
        num_merges, merge_keys_list, merge_keys_list_sizes,
        num_merges, merge_values_list, merge_values_list_sizes);
    rocksdb_writebatch_deletev(b,
        num_deletes, delete_keys_list, delete_keys_list_sizes);

    char *errMsg = NULL;

    rocksdb_writeoptions_t *options = rocksdb_writeoptions_create();

    rocksdb_write(db, options, b, &errMsg);

    rocksdb_writeoptions_destroy(options);

    rocksdb_writebatch_destroy(b);

    return errMsg;
}
*/
import "C"

import (
	"errors"
	"unsafe"

	"github.com/blevesearch/bleve/index/store"
)

func (s *Store) ExecuteDirectBatch(
	sets []store.KVDirectKeyVal,
	merges []store.KVDirectKeyVal,
	deletes [][]byte,
) error {
	var num_sets C.int
	var set_keys_list **C.char
	var set_keys_list_sizes *C.size_t
	var set_values_list **C.char
	var set_values_list_sizes *C.size_t

	var num_merges C.int
	var merge_keys_list **C.char
	var merge_keys_list_sizes *C.size_t
	var merge_values_list **C.char
	var merge_values_list_sizes *C.size_t

	var num_deletes C.int
	var delete_keys_list **C.char
	var delete_keys_list_sizes *C.size_t

	if len(sets) > 0 {
		setKeys := make([]*C.char, len(sets))
		setKeyLens := make([]C.size_t, len(sets))
		setVals := make([]*C.char, len(sets))
		setValLens := make([]C.size_t, len(sets))

		for i, kv := range sets {
			setKeys[i] = (*C.char)(unsafe.Pointer(&kv.Key[0]))
			setKeyLens[i] = (C.size_t)(len(kv.Key))
			setVals[i] = (*C.char)(unsafe.Pointer(&kv.Val[0]))
			setValLens[i] = (C.size_t)(len(kv.Val))
		}

		num_sets = (C.int)(len(sets))
		set_keys_list = (**C.char)(unsafe.Pointer(&setKeys[0]))
		set_keys_list_sizes = (*C.size_t)(unsafe.Pointer(&setKeyLens[0]))
		set_values_list = (**C.char)(unsafe.Pointer(&setVals[0]))
		set_values_list_sizes = (*C.size_t)(unsafe.Pointer(&setValLens[0]))
	}

	if len(merges) > 0 {
		mergeKeys := make([]*C.char, len(merges))
		mergeKeyLens := make([]C.size_t, len(merges))
		mergeVals := make([]*C.char, len(merges))
		mergeValLens := make([]C.size_t, len(merges))

		for i, kv := range merges {
			mergeKeys[i] = (*C.char)(unsafe.Pointer(&kv.Key[0]))
			mergeKeyLens[i] = (C.size_t)(len(kv.Key))
			mergeVals[i] = (*C.char)(unsafe.Pointer(&kv.Val[0]))
			mergeValLens[i] = (C.size_t)(len(kv.Val))
		}

		num_merges = (C.int)(len(merges))
		merge_keys_list = (**C.char)(unsafe.Pointer(&mergeKeys[0]))
		merge_keys_list_sizes = (*C.size_t)(unsafe.Pointer(&mergeKeyLens[0]))
		merge_values_list = (**C.char)(unsafe.Pointer(&mergeVals[0]))
		merge_values_list_sizes = (*C.size_t)(unsafe.Pointer(&mergeValLens[0]))
	}

	if len(deletes) > 0 {
		deleteKeys := make([]*C.char, len(deletes))
		deleteKeyLens := make([]C.size_t, len(deletes))

		for i, k := range deletes {
			deleteKeys[i] = (*C.char)(unsafe.Pointer(&k[0]))
			deleteKeyLens[i] = (C.size_t)(len(k))
		}

		num_deletes = (C.int)(len(deletes))
		delete_keys_list = (**C.char)(unsafe.Pointer(&deleteKeys[0]))
		delete_keys_list_sizes = (*C.size_t)(unsafe.Pointer(&deleteKeyLens[0]))
	}

	cErr := C.blevex_rocksdb_execute_direct_batch(
		(*C.rocksdb_t)(s.db.UnsafeGetDB()),
		num_sets,
		set_keys_list,
		set_keys_list_sizes,
		set_values_list,
		set_values_list_sizes,
		num_merges,
		merge_keys_list,
		merge_keys_list_sizes,
		merge_values_list,
		merge_values_list_sizes,
		num_deletes,
		delete_keys_list,
		delete_keys_list_sizes)
	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return err
	}

	return nil
}
