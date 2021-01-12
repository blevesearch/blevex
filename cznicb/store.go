//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

// Package cznicb provides an in-memory implementation of the KVStore
// interfaces using the cznic/b in-memory btree.  Of note: this
// implementation does not have reader isolation.
package cznicb

import (
	"bytes"
	"fmt"
	"os"
	"sync"

	store "github.com/blevesearch/upsidedown_store_api"
	"github.com/blevesearch/bleve/v2/registry"

	"github.com/cznic/b"
)

const Name = "cznicb"

type Store struct {
	m  sync.RWMutex
	t  *b.Tree
	mo store.MergeOperator
}

func itemCompare(a, b interface{}) int {
	return bytes.Compare(a.([]byte), b.([]byte))
}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}
	if path != "" {
		return nil, os.ErrInvalid
	}
	s := &Store{
		t:  b.TreeNew(itemCompare),
		mo: mo,
	}
	return s, nil
}

func (s *Store) Close() error {
	return nil
}

func (s *Store) Reader() (store.KVReader, error) {
	return &Reader{s: s}, nil
}

func (s *Store) Writer() (store.KVWriter, error) {
	return &Writer{s: s}, nil
}

func init() {
	registry.RegisterKVStore(Name, New)
}
