//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rocksdb

import (
	"bytes"

	"github.com/tecbot/gorocksdb"
)

type Iterator struct {
	store    *Store
	iterator *gorocksdb.Iterator

	prefix []byte
	start  []byte
	end    []byte
}

func (i *Iterator) Seek(key []byte) {
	if bytes.Compare(key, i.start) < 0 {
		key = i.start
	}
	i.iterator.Seek(key)
}

func (i *Iterator) Next() {
	i.iterator.Next()
}

func (i *Iterator) Current() ([]byte, []byte, bool) {
	if i.Valid() {
		return i.Key(), i.Value(), true
	}
	return nil, nil, false
}

func (i *Iterator) Key() []byte {
	return i.iterator.Key().Data()
}

func (i *Iterator) Value() []byte {
	return i.iterator.Value().Data()
}

func (i *Iterator) Valid() bool {
	if !i.iterator.Valid() {
		return false
	} else if i.prefix != nil && !bytes.HasPrefix(i.iterator.Key().Data(), i.prefix) {
		return false
	} else if i.end != nil && bytes.Compare(i.iterator.Key().Data(), i.end) >= 0 {
		return false
	}

	return true
}

func (i *Iterator) Close() error {
	i.iterator.Close()
	return nil
}
