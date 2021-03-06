package bleve

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2"
)

func TestIndexBatcherConcurrentCrud(t *testing.T) {
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	index, err := bleve.New("testidx", bleve.NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}

	index = NewIndexBatcher(index, 2*time.Millisecond)

	{
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			doca := map[string]interface{}{
				"name": "marty",
				"desc": "gophercon india",
			}
			err2 := index.Index("a", doca)
			if err2 != nil {
				t.Error(err2)
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			docy := map[string]interface{}{
				"name": "jasper",
				"desc": "clojure",
			}
			err3 := index.Index("y", docy)
			if err3 != nil {
				t.Error(err3)
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			docy := map[string]interface{}{
				"name": "jasper2",
				"desc": "clojure2",
			}
			err4 := index.Index("y2", docy)
			if err4 != nil {
				t.Error(err4)
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			err5 := index.SetInternal([]byte("status2"), []byte("pending"))
			if err5 != nil {
				t.Error(err5)
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			docx := map[string]interface{}{
				"name": "rose",
				"desc": "googler",
			}
			err6 := index.Index("x", docx)
			if err6 != nil {
				t.Error(err6)
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			err7 := index.SetInternal([]byte("status"), []byte("pending"))
			if err7 != nil {
				t.Error(err7)
			}
			wg.Done()
		}()

		wg.Wait()
	}

	val, err := index.GetInternal([]byte("status2"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "pending" {
		t.Errorf("expected pending, got '%s'", val)
	}

	{
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			err8 := index.Delete("y")
			if err8 != nil {
				t.Error(err8)
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			err9 := index.Delete("y2")
			if err9 != nil {
				t.Error(err9)
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			err10 := index.DeleteInternal([]byte("status2"))
			if err10 != nil {
				t.Error(err10)
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			err11 := index.SetInternal([]byte("status"), []byte("ready"))
			if err11 != nil {
				t.Error(err11)
			}
			wg.Done()
		}()

		wg.Wait()
	}

	val, err = index.GetInternal([]byte("status2"))
	if err != nil {
		t.Error(err)
	}
	if val != nil {
		t.Errorf("expected nil, got '%s'", val)
	}

	docb := map[string]interface{}{
		"name": "steve",
		"desc": "cbft master",
	}
	batch := index.NewBatch()
	err = batch.Index("b", docb)
	if err != nil {
		t.Error(err)
	}
	batch.Delete("x")
	batch.SetInternal([]byte("batchi"), []byte("batchv"))
	batch.DeleteInternal([]byte("status"))
	err = index.Batch(batch)
	if err != nil {
		t.Error(err)
	}
	val, err = index.GetInternal([]byte("batchi"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "batchv" {
		t.Errorf("expected 'batchv', got '%s'", val)
	}
	val, err = index.GetInternal([]byte("status"))
	if err != nil {
		t.Error(err)
	}
	if val != nil {
		t.Errorf("expected nil, got '%s'", val)
	}

	{
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			err12 := index.SetInternal([]byte("seqno"), []byte("7"))
			if err12 != nil {
				t.Error(err12)
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			err13 := index.DeleteInternal([]byte("status"))
			if err13 != nil {
				t.Error(err13)
			}
			wg.Done()
		}()

		wg.Wait()
	}
	val, err = index.GetInternal([]byte("status"))
	if err != nil {
		t.Error(err)
	}
	if val != nil {
		t.Errorf("expected nil, got '%s'", val)
	}

	val, err = index.GetInternal([]byte("seqno"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "7" {
		t.Errorf("expected '7', got '%s'", val)
	}

	count, err := index.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("expected doc count 2, got %d", count)
	}

	doc, err := index.Document("a")
	if err != nil {
		t.Fatal(err)
	}
	if doc == nil {
		t.Errorf("expected doc not nil, got nil")
	}

	doc, err = index.Document("y2")
	if err != nil {
		t.Fatal(err)
	}
	if doc != nil {
		t.Errorf("expected doc nil, got not nil")
	}
}
