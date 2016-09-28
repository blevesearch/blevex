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
	"encoding/binary"
	"fmt"
	"io"
)

// Writer writes KVPairs
type Writer struct {
	w    io.Writer
	buf  []byte
	sbuf []byte
}

// NewWriter returns a KVPair Writer which writes to the provided Writer
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w:    w,
		buf:  make([]byte, 0, 1024),
		sbuf: make([]byte, binary.MaxVarintLen64),
	}
}

func (w *Writer) Write(p *KVPair) error {
	kvpsize := p.Size()
	if cap(w.buf) < kvpsize {
		w.buf = make([]byte, 0, kvpsize)
	}
	n, err := p.MarshalTo(w.buf[:kvpsize])
	if err != nil {
		return fmt.Errorf("error marshaling row: %v", err)
	}
	sn := binary.PutUvarint(w.sbuf, uint64(kvpsize))
	_, err = w.w.Write(w.sbuf[:sn])
	if err != nil {
		return fmt.Errorf("error writing row size: %v", err)
	}
	_, err = w.w.Write(w.buf[:n])
	if err != nil {
		return fmt.Errorf("error writing row data: %v", err)
	}
	return nil
}
