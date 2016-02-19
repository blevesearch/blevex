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
	"fmt"
	"unsafe"

	"github.com/blevesearch/bleve/index/store"
)

type Writer struct {
	store   *Store
	options *C.rocksdb_writeoptions_t
}

func (w *Writer) NewBatch() store.KVBatch {
	rv := Batch{
		batch: C.rocksdb_writebatch_create(),
	}
	return &rv
}

func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	// NOTE: We've reverted to old, emulated batch due to apparent corruption.
	//
	// rv := newBatchEx(options)
	// return rv.buf, rv, nil
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

func (w *Writer) ExecuteBatch(b store.KVBatch) error {
	batchex, ok := b.(*BatchEx)
	if ok {
		return batchex.execute(w)
	}
	batch, ok := b.(*Batch)
	if ok {
		var cErr *C.char
		C.rocksdb_write(w.store.db, w.options, batch.batch, &cErr)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))

			return errors.New(C.GoString(cErr))
		}

		return nil
	}
	return fmt.Errorf("wrong type of batch")
}

func (w *Writer) Close() error {
	C.rocksdb_writeoptions_destroy(w.options)
	w.options = nil
	return nil
}
