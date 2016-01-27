//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rocksdb

// #cgo CXXFLAGS: -std=c++11
// #cgo CPPFLAGS: -I ../../../cockroachdb/c-rocksdb/internal/include
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
// #include <stdlib.h>
// #include "rocksdb/c.h"
// #include "merge.h"
/*

*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"

	_ "github.com/cockroachdb/c-rocksdb"
)

const Name = "rocksdb"

type Store struct {
	path   string
	opts   *C.rocksdb_options_t
	config map[string]interface{}
	db     *C.rocksdb_t
	mo     *C.rocksdb_mergeoperator_t
	gomo   store.MergeOperator // hold refernce to it to prevent GC

	roptVerifyChecksums    bool
	roptVerifyChecksumsUse bool
	roptFillCache          bool
	roptFillCacheUse       bool
	roptReadTier           int
	roptReadTierUse        bool

	woptSync          bool
	woptSyncUse       bool
	woptDisableWAL    bool
	woptDisableWALUse bool
}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {

	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}

	rv := Store{
		path:   path,
		config: config,
		opts:   C.rocksdb_options_create(),
	}

	// install merge operator, prefer C version if available
	if mo != nil {
		rv.gomo = mo
		if moc, ok := mo.(store.NativeMergeOperator); ok {
			rv.mo = C.native_mergeoperator_create(moc.FullMergeC(), moc.PartialMergeC(), moc.NameC())
			C.rocksdb_options_set_merge_operator(rv.opts, rv.mo)
		} else {
			state := unsafe.Pointer(&mo)
			rv.mo = C.go_mergeoperator_create(state)
			C.rocksdb_options_set_merge_operator(rv.opts, rv.mo)
		}
	}

	_, err := applyConfig(rv.opts, config)
	if err != nil {
		return nil, err
	}

	var cErr *C.char
	cname := C.CString(rv.path)
	defer C.free(unsafe.Pointer(cname))
	rv.db = C.rocksdb_open(rv.opts, cname, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))

		return nil, errors.New(C.GoString(cErr))
	}

	b, ok := config["readoptions_verify_checksum"].(bool)
	if ok {
		rv.roptVerifyChecksums, rv.roptVerifyChecksumsUse = b, true
	}

	b, ok = config["readoptions_fill_cache"].(bool)
	if ok {
		rv.roptFillCache, rv.roptFillCacheUse = b, true
	}

	v, ok := config["readoptions_read_tier"].(float64)
	if ok {
		rv.roptReadTier, rv.roptReadTierUse = int(v), true
	}

	b, ok = config["writeoptions_sync"].(bool)
	if ok {
		rv.woptSync, rv.woptSyncUse = b, true
	}

	b, ok = config["writeoptions_disable_WAL"].(bool)
	if ok {
		rv.woptDisableWAL, rv.woptDisableWALUse = b, true
	}

	return &rv, nil
}

func (s *Store) Close() error {
	C.rocksdb_close(s.db)
	s.db = nil
	C.rocksdb_options_destroy(s.opts)
	s.opts = nil
	return nil
}

func (s *Store) Reader() (store.KVReader, error) {
	snapshot := C.rocksdb_create_snapshot(s.db)
	options := s.newReadOptions()
	C.rocksdb_readoptions_set_snapshot(options, snapshot)
	return &Reader{
		store:    s,
		snapshot: snapshot,
		options:  options,
	}, nil
}

func (s *Store) Writer() (store.KVWriter, error) {
	return &Writer{
		store:   s,
		options: s.newWriteOptions(),
	}, nil
}

func init() {
	registry.RegisterKVStore(Name, New)
}
