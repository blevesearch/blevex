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
	"fmt"
	"io"

	"github.com/blevesearch/bleve/index"
)

// ExportBleve will dump all the index rows from
// the provided index and serialize them to the
// provided Writer
func ExportBleve(i index.Index, w io.Writer) error {
	kvpw := NewWriter(w)

	r, err := i.Reader()
	if err != nil {
		return fmt.Errorf("error getting reader: %v", err)
	}

	var dumpChan chan interface{}
	dumpChan = r.DumpAll()

	for dumpValue := range dumpChan {
		switch dumpValue := dumpValue.(type) {
		case index.IndexRow:
			p := KVPair{K: dumpValue.Key(), V: dumpValue.Value()}
			err = kvpw.Write(&p)
			if err != nil {
				return fmt.Errorf("error writing row: %v", err)
			}

		case error:
			return fmt.Errorf("error dumping row: %v", dumpValue)
		}
	}
	return nil
}
