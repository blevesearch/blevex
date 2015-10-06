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
	"fmt"

	"github.com/blevesearch/bleve/index/store"
	"github.com/couchbase/goforestdb"
)

type Writer struct {
	store   *Store
	kvstore *forestdb.KVStore
}

func (w *Writer) NewBatch() store.KVBatch {
	return store.NewEmulatedBatch(w.store.mo)
}

func (w *Writer) ExecuteBatch(batch store.KVBatch) error {

	emulatedBatch, ok := batch.(*store.EmulatedBatch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}

	w.store.m.Lock()
	defer w.store.m.Unlock()

	for key, mergeOps := range emulatedBatch.Merger.Merges {
		k := []byte(key)
		ob, err := w.kvstore.GetKV(k)
		if err != nil && err != forestdb.RESULT_KEY_NOT_FOUND {
			return err
		}
		mergedVal, fullMergeOk := w.store.mo.FullMerge(k, ob, mergeOps)
		if !fullMergeOk {
			return fmt.Errorf("merge operator returned failure")
		}
		err = w.kvstore.SetKV(k, mergedVal)
		if err != nil {
			return err
		}
	}

	for _, op := range emulatedBatch.Ops {
		if op.V != nil {
			err := w.kvstore.SetKV(op.K, op.V)
			if err != nil {
				return err
			}
		} else {
			err := w.kvstore.DeleteKV(op.K)
			if err != nil {
				return err
			}
		}
	}

	w.kvstore.File().Commit(forestdb.COMMIT_NORMAL)
	return nil
}

func (w *Writer) Close() error {
	return w.store.kvpool.Return(w.kvstore)
}
