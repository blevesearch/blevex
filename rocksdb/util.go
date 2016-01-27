package rocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"

import (
	"reflect"
	"unsafe"
)

// Originally from github.com/tecbot/gorocksdb/util.go.
func unsafeToByteSlice(data unsafe.Pointer, len int) []byte {
	var value []byte

	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = len, len, uintptr(data)

	return value
}

func unsafeToCPtrCharSlice(data unsafe.Pointer, len int) []*C.char {
	var value []*C.char

	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = len, len, uintptr(data)

	return value
}

func unsafeToCSizeTSlice(data unsafe.Pointer, len int) []C.size_t {
	var value []C.size_t

	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = len, len, uintptr(data)

	return value
}

func byteToChar(b []byte) *C.char {
	var c *C.char
	if len(b) > 0 {
		c = (*C.char)(unsafe.Pointer(&b[0]))
	}

	return c
}

func charToByte(data *C.char, len C.size_t) []byte {
	var value []byte

	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = int(len), int(len), uintptr(unsafe.Pointer(data))

	return value
}

func boolToChar(b bool) C.uchar {
	if b {
		return 1
	}
	return 0
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// stringToChar returns *C.char from string.
func stringToChar(s string) *C.char {
	ptrStr := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return (*C.char)(unsafe.Pointer(ptrStr.Data))
}

// charSlice converts a C array of *char to a []*C.char.
func charSlice(data **C.char, len C.int) []*C.char {
	var value []*C.char
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = int(len), int(len), uintptr(unsafe.Pointer(data))
	return value
}

// sizeSlice converts a C array of size_t to a []C.size_t.
func sizeSlice(data *C.size_t, len C.int) []C.size_t {
	var value []C.size_t
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = int(len), int(len), uintptr(unsafe.Pointer(data))
	return value
}
