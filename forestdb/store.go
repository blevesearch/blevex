//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package forestdb

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
	"github.com/couchbase/goforestdb"
)

const Name = "forestdb"
const DefaultConcurrent = 10

type Store struct {
	m      sync.RWMutex
	path   string
	kvpool *forestdb.KVPool
	mo     store.MergeOperator

	statsMutex  sync.Mutex
	statsHandle *forestdb.KVStore
}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {

	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}

	forestDBDefaultConfig := forestdb.DefaultConfig()
	forestDBDefaultConfig.SetCompactionMode(forestdb.COMPACT_AUTO)
	forestDBDefaultConfig.SetMultiKVInstances(false)
	forestDBConfig, err := applyConfig(forestDBDefaultConfig, config)
	if err != nil {
		return nil, err
	}

	kvconfig := forestdb.DefaultKVStoreConfig()
	if cim, ok := config["create_if_missing"].(bool); ok && cim {
		kvconfig.SetCreateIfMissing(true)
	}

	numConcurrent := DefaultConcurrent
	if nc, ok := config["num_concurrent"].(float64); ok {
		numConcurrent = int(nc)
	}

	// request 1 extra connection in pool to be reserved for issuing
	// stats calls
	kvpool, err := forestdb.NewKVPool(path, forestDBConfig, "default", kvconfig, numConcurrent+1)
	if err != nil {
		return nil, err
	}

	rv := Store{
		path:   path,
		mo:     mo,
		kvpool: kvpool,
	}

	rv.statsHandle, err = kvpool.Get()
	if err != nil {
		return nil, err
	}

	return &rv, nil
}

func (s *Store) Close() error {
	if s.statsHandle != nil {
		s.kvpool.Return(s.statsHandle)
	}
	return s.kvpool.Close()
}

func (s *Store) Reader() (store.KVReader, error) {
	kvstore, err := s.kvpool.Get()
	if err != nil {
		return nil, err
	}
	snapshot, err := kvstore.SnapshotOpen(forestdb.SnapshotInmem)
	if err != nil {
		return nil, err
	}
	return &Reader{
		store:    s,
		kvstore:  kvstore,
		snapshot: snapshot,
	}, nil
}

func (s *Store) Stats() json.Marshaler {
	return &kvStat{
		s: s,
	}
}

func (s *Store) Writer() (store.KVWriter, error) {
	kvstore, err := s.kvpool.Get()
	if err != nil {
		return nil, err
	}
	return &Writer{
		store:   s,
		kvstore: kvstore,
	}, nil
}

func init() {
	registry.RegisterKVStore(Name, New)
}
