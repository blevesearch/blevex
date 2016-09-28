//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package preload

import (
	"io"

	"github.com/blevesearch/bleve/index/store"
)

// Import reads KVPairs from the Reader
// and sets them in the KVStore
// all work is done in batches of the requested size
func Import(s store.KVStore, r io.Reader, batchSize int) error {
	kvw, err := s.Writer()
	if err != nil {
		return err
	}
	kvpr := NewReader(r)
	b := kvw.NewBatch()
	bsize := 0
	p := &KVPair{}
	p, err = kvpr.Read(p)
	for err == nil {
		b.Set(p.K, p.V)
		bsize++
		if bsize > batchSize {
			err = kvw.ExecuteBatch(b)
			if err != nil {
				return err
			}
			bsize = 0
			b = kvw.NewBatch()
		}
		p, err = kvpr.Read(p)
	}
	if err != nil && err != io.EOF {
		return err
	}
	// close last batch
	if bsize > 0 {
		err = kvw.ExecuteBatch(b)
		if err != nil {
			return err
		}
	}
	return nil
}
