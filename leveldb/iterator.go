//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package leveldb

import (
	"bytes"

	"github.com/jmhodges/levigo"
)

type Iterator struct {
	store    *Store
	iterator *levigo.Iterator

	prefix []byte
	end    []byte
}

func (i *Iterator) Seek(key []byte) {
	if key == nil {
		key = []byte{0}
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
	return i.iterator.Key()
}

func (i *Iterator) Value() []byte {
	return i.iterator.Value()
}

func (i *Iterator) Valid() bool {
	if !i.iterator.Valid() {
		return false
	} else if i.prefix != nil && !bytes.HasPrefix(i.iterator.Key(), i.prefix) {
		return false
	} else if i.end != nil && bytes.Compare(i.iterator.Key(), i.end) >= 0 {
		return false
	}
	return true
}

func (i *Iterator) Close() error {
	i.iterator.Close()
	return nil
}
