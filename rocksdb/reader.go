//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"

import (
	"errors"
	"unsafe"

	"github.com/blevesearch/bleve/index/store"
)

type Reader struct {
	store    *Store
	snapshot *C.rocksdb_snapshot_t
	options  *C.rocksdb_readoptions_t
}

// Get will return values which are bytes copied from C to Go, you own them
func (r *Reader) Get(key []byte) ([]byte, error) {
	cKey := byteToChar(key)

	var cErr *C.char
	var cValLen C.size_t
	cValue := C.rocksdb_get(r.store.db, r.options, cKey, C.size_t(len(key)), &cValLen, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}

	if cValue == nil {
		return nil, nil
	}

	defer C.free(unsafe.Pointer(cValue))
	return C.GoBytes(unsafe.Pointer(cValue), C.int(cValLen)), nil

}

func (r *Reader) MultiGet(keys [][]byte) ([][]byte, error) {
	return store.MultiGet(r, keys)
}

func (r *Reader) PrefixIterator(prefix []byte) store.KVIterator {
	rv := Iterator{
		store:    r.store,
		iterator: C.rocksdb_create_iterator(r.store.db, r.options),
		prefix:   prefix,
	}
	rv.Seek(prefix)
	return &rv
}

func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	rv := Iterator{
		store:    r.store,
		iterator: C.rocksdb_create_iterator(r.store.db, r.options),
		start:    start,
		end:      end,
	}
	rv.Seek(start)
	return &rv
}

func (r *Reader) Close() error {
	C.rocksdb_readoptions_destroy(r.options)
	r.options = nil
	C.rocksdb_release_snapshot(r.store.db, r.snapshot)
	r.snapshot = nil
	return nil
}
