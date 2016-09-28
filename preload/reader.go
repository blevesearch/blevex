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
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
)

// Reader reads KVPairs
type Reader struct {
	r   *bufio.Reader
	buf []byte
}

// NewReader creates a new KVPair reader
// that reads from the provided reader
func NewReader(r io.Reader) *Reader {
	return &Reader{
		r:   bufio.NewReader(r),
		buf: make([]byte, 1024),
	}
}

// Read will read the next KVPair into p
// if p is nil a new KVPair is allocated
func (r *Reader) Read(p *KVPair) (*KVPair, error) {
	if p == nil {
		p = &KVPair{}
	}
	size, err := binary.ReadUvarint(r.r)
	if err != nil {
		return nil, err
	}
	if cap(r.buf) < int(size) {
		r.buf = make([]byte, 0, size)
	}
	read, err := io.ReadFull(r.r, r.buf[:size]) //r.r.Read(r.buf[:size])
	if err != nil {
		return nil, err
	}
	if read != int(size) {
		return nil, fmt.Errorf("read incomplete kv pair")
	}
	err = proto.Unmarshal(r.buf[:size], p)
	if err != nil {
		return nil, err
	}
	return p, nil
}
