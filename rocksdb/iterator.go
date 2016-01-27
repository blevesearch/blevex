//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rocksdb

// #include "rocksdb/c.h"
import "C"

import "bytes"

type Iterator struct {
	store    *Store
	iterator *C.rocksdb_iterator_t

	prefix []byte
	start  []byte
	end    []byte
}

func (i *Iterator) Seek(key []byte) {
	if i.start != nil && bytes.Compare(key, i.start) < 0 {
		key = i.start
	}
	if i.prefix != nil && !bytes.HasPrefix(key, i.prefix) {
		if bytes.Compare(key, i.prefix) < 0 {
			key = i.prefix
		} else {
			var end []byte
			for x := len(i.prefix) - 1; x >= 0; x-- {
				c := i.prefix[x]
				if c < 0xff {
					end = make([]byte, x+1)
					copy(end, i.prefix)
					end[x] = c + 1
					break
				}
			}
			key = end
		}
	}
	cKey := byteToChar(key)
	C.rocksdb_iter_seek(i.iterator, cKey, C.size_t(len(key)))
}

func (i *Iterator) Next() {
	C.rocksdb_iter_next(i.iterator)
}

func (i *Iterator) Current() ([]byte, []byte, bool) {
	if i.Valid() {
		return i.Key(), i.Value(), true
	}
	return nil, nil, false
}

func (i *Iterator) Key() []byte {
	var cLen C.size_t
	cKey := C.rocksdb_iter_key(i.iterator, &cLen)
	if cKey == nil {
		return nil
	}
	return charToByte(cKey, cLen)
}

func (i *Iterator) Value() []byte {
	var cLen C.size_t
	cKey := C.rocksdb_iter_value(i.iterator, &cLen)
	if cKey == nil {
		return nil
	}

	return charToByte(cKey, cLen)
}

func (i *Iterator) Valid() bool {
	if C.rocksdb_iter_valid(i.iterator) == 0 {
		return false
	} else if i.prefix != nil && !bytes.HasPrefix(i.Key(), i.prefix) {
		return false
	} else if i.end != nil && bytes.Compare(i.Key(), i.end) >= 0 {
		return false
	}
	return true
}

func (i *Iterator) Close() error {
	C.rocksdb_iter_destroy(i.iterator)
	i.iterator = nil
	return nil
}
