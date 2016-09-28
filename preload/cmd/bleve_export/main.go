//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"compress/gzip"
	"flag"
	"log"
	"os"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/blevex/preload"

	_ "github.com/blevesearch/bleve/config"
)

func main() {
	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatalf("must specify path to index")
	}
	if flag.NArg() < 2 {
		log.Fatalf("must specify path to export to")
	}
	i, err := bleve.Open(flag.Arg(0))
	if err != nil {
		log.Fatalf("error opening index: %v", err)
	}
	f, err := os.OpenFile(flag.Arg(1), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		log.Fatalf("error opening export path: %v", err)
	}
	ii, _, err := i.Advanced()
	if err != nil {
		log.Fatalf("error getting internal index: %v", err)
	}
	gzf := gzip.NewWriter(f)
	err = preload.ExportBleve(ii, gzf)
	if err != nil {
		log.Fatalf("error exporting bleve index: %v", err)
	}
	err = gzf.Close()
	if err != nil {
		log.Fatalf("error closing gzip: %v", err)
	}
	err = f.Close()
	if err != nil {
		log.Fatalf("error closing export file: %v", err)
	}
	err = i.Close()
	if err != nil {
		log.Fatalf("error closing index: %v", err)
	}
}
