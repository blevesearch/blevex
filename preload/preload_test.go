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
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func TestRoundtrip(t *testing.T) {
	data := []*KVPair{
		&KVPair{
			K: []byte("cat"),
			V: []byte("taffy"),
		},
	}

	tmp, err := ioutil.TempFile("/tmp", "blevekvp")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll(tmp.Name())
		if err != nil {
			t.Fatal(err)
		}
	}()

	kvpw := NewWriter(tmp)
	for _, d := range data {
		err = kvpw.Write(d)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = tmp.Close()
	if err != nil {
		t.Fatal(err)
	}

	tmp, err = os.Open(tmp.Name())
	if err != nil {
		t.Fatal(err)
	}

	kvpr := NewReader(tmp)
	read := 0
	kvp := &KVPair{}
	kvp, err = kvpr.Read(kvp)
	for err == nil {
		if !reflect.DeepEqual(kvp, data[read]) {
			t.Errorf("expected %v got %v", data[read], kvp)
		}
		read++
		kvp, err = kvpr.Read(kvp)
	}
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}

}
