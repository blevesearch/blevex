//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rocksdb

// #include <rocksdb/c.h>
import "C"

type Batch struct {
	batch *C.rocksdb_writebatch_t
}

func (b *Batch) Set(key, val []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(val)
	C.rocksdb_writebatch_put(b.batch, cKey, C.size_t(len(key)), cValue, C.size_t(len(val)))
}

func (b *Batch) Delete(key []byte) {
	cKey := byteToChar(key)
	C.rocksdb_writebatch_delete(b.batch, cKey, C.size_t(len(key)))
}

func (b *Batch) Merge(key, val []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(val)
	C.rocksdb_writebatch_merge(b.batch, cKey, C.size_t(len(key)), cValue, C.size_t(len(val)))
}

func (b *Batch) Reset() {
	C.rocksdb_writebatch_clear(b.batch)
}

func (b *Batch) Close() error {
	C.rocksdb_writebatch_destroy(b.batch)
	b.batch = nil
	return nil
}
