package test

// #include <stdlib.h>
// #include <strings.h>
/*
char* merge_operator_partial_merge_fn(
  void *state,
  const char *key, size_t key_length,
  const char* const *operands_list, const size_t *operands_list_length,
  int num_operands,
  unsigned char *success, size_t *new_value_length)
{
  uint64_t newval = 0;

  for (int i = 0; i < num_operands; i++) {
      if (operands_list_length[i] == sizeof(uint64_t)) {
          uint64_t next;
          memcpy(&next, operands_list[i], sizeof(next));
          newval += next;
      } else {
          *success = 0;
          *new_value_length = 0;
          return NULL;
      }
  }

  // result must be malloc'ed here (freed by rocksdb)
  char *result = malloc(sizeof(uint64_t));
  memcpy(result, &newval, sizeof(uint64_t));
  if (!result) {
      *success = 0;
      *new_value_length = 0;
      return NULL;
  }
  *success = 1;
  *new_value_length = sizeof(result);
  return result;
}

char* merge_operator_full_merge_fn (
    void *state,
    const char *key, size_t key_length,
    const char *existing_value, size_t existing_value_length,
    const char *const *operands_list, const size_t *operands_list_length,
    int num_operands,
    unsigned char *success, size_t *new_value_length)
{
    uint64_t newval = 0;
    if (existing_value_length == sizeof(uint64_t)) {
        memcpy(&newval, existing_value, sizeof(newval));
    }

    for (int i = 0; i < num_operands; i++) {
        if (operands_list_length[i] == sizeof(uint64_t)) {
            uint64_t next;
            memcpy(&next, operands_list[i], sizeof(next));
            newval += next;
        } else {
            *success = 0;
            *new_value_length = 0;
            return NULL;
        }
    }

    // result must be malloc'ed here (freed by rocksdb)
    char *result = malloc(sizeof(uint64_t));
    memcpy(result, &newval, sizeof(uint64_t));
    if (!result) {
        *success = 0;
        *new_value_length = 0;
        return NULL;
    }
    *success = 1;
    *new_value_length = sizeof(result);
    return result;
}
const char* merge_operator_name_fn(void *state)
{
    return "tesbdb_merge_op";
}
*/
import "C"

import (
	"encoding/binary"
	"testing"
	"unsafe"

	"github.com/blevesearch/bleve/index/store"
)

// TestMergeCounter is just an incrementing counter of uint64
type TestMergeCounter struct{}

func (mc *TestMergeCounter) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	var newval uint64
	if len(existingValue) > 0 {
		newval = binary.LittleEndian.Uint64(existingValue)
	}

	// now process operands
	for _, operand := range operands {
		next := binary.LittleEndian.Uint64(operand)
		newval += next
	}

	rv := make([]byte, 8)
	binary.LittleEndian.PutUint64(rv, newval)
	return rv, true
}

func (mc *TestMergeCounter) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	left := binary.LittleEndian.Uint64(leftOperand)
	right := binary.LittleEndian.Uint64(rightOperand)
	rv := make([]byte, 8)
	binary.LittleEndian.PutUint64(rv, left+right)
	return rv, true
}

func (mc *TestMergeCounter) Name() string {
	return "test_merge_counter"
}

// native C impls
func (mc *TestMergeCounter) FullMergeC() unsafe.Pointer {
	return unsafe.Pointer(C.merge_operator_full_merge_fn)
}

func (mc *TestMergeCounter) PartialMergeC() unsafe.Pointer {
	return unsafe.Pointer(C.merge_operator_partial_merge_fn)
}

func (mc *TestMergeCounter) NameC() unsafe.Pointer {
	return unsafe.Pointer(C.merge_operator_name_fn)
}

// test merge behavior

func encodeUint64(in uint64) []byte {
	rv := make([]byte, 8)
	binary.LittleEndian.PutUint64(rv, in)
	return rv
}

func CommonTestMergeNative(t *testing.T, s store.KVStore, invocation int) {

	testKey := []byte("k1")

	data := []struct {
		key []byte
		val []byte
	}{
		{testKey, encodeUint64(1)},
		{testKey, encodeUint64(1)},
	}

	// open a writer
	writer, err := s.Writer()
	if err != nil {
		t.Fatal(err)
	}

	batch := writer.NewBatch()
	for _, row := range data {
		batch.Merge(row.key, row.val)
	}
	err = writer.ExecuteBatch(batch)
	if err != nil {
		t.Fatal(err)
	}

	// close the writer
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}

	// open a reader
	reader, err := s.Reader()
	if err != nil {
		t.Fatal(err)
	}

	// read key
	returnedVal, err := reader.Get(testKey)
	if err != nil {
		t.Fatal(err)
	}

	// check the value
	mergedval := binary.LittleEndian.Uint64(returnedVal)
	expected := uint64(2 * invocation)
	if mergedval != expected {
		t.Errorf("expected %d, got %d", expected, mergedval)
	}

	// close the reader
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

}
