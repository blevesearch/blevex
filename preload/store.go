//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

//go:generate protoc --gofast_out=. kvutil.proto

package preload

import (
	"compress/gzip"
	"fmt"
	"os"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
)

const Name = "preload"

type Store struct {
	o store.KVStore
}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	name, ok := config["kvStoreName_actual"].(string)
	if !ok || name == "" {
		return nil, fmt.Errorf("preload: missing kvStoreName_actual,"+
			" config: %#v", config)
	}

	if name == Name {
		return nil, fmt.Errorf("preload: circular kvStoreName_actual")
	}

	ctr := registry.KVStoreConstructorByName(name)
	if ctr == nil {
		return nil, fmt.Errorf("preload: no kv store constructor,"+
			" kvStoreName_actual: %s", name)
	}

	kvs, err := ctr(mo, config)
	if err != nil {
		return nil, err
	}

	rv := &Store{
		o: kvs,
	}

	if preloadPath, ok := config["preloadpath"].(string); ok {
		f, err := os.Open(preloadPath)
		if err != nil {
			return nil, err
		}
		gzr, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		err = Import(rv, gzr, 1024)
		if err != nil {
			return nil, err
		}
		err = gzr.Close()
		if err != nil {
			return nil, err
		}
		err = f.Close()
		if err != nil {
			return nil, err
		}
	}

	return rv, nil
}

func (s *Store) Close() error {
	return s.o.Close()
}

func (s *Store) Reader() (store.KVReader, error) {
	return s.o.Reader()
}

func (s *Store) Writer() (store.KVWriter, error) {
	return s.o.Writer()
}

func init() {
	registry.RegisterKVStore(Name, New)
}
