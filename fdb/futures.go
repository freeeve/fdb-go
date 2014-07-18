// FoundationDB Go API
// Copyright (c) 2013 FoundationDB, LLC

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package fdb

/*
 #cgo LDFLAGS: -lfdb_c -lm
 #define FDB_API_VERSION 200
 #include <foundationdb/fdb_c.h>
 #include <string.h>

 extern void notifyChannel(void*);

 void go_callback(FDBFuture* f, void* ch) {
     notifyChannel(ch);
 }

 void go_set_callback(void* f, void* ch) {
     fdb_future_set_callback(f, (FDBCallback)&go_callback, ch);
 }
*/
import "C"

import (
	"runtime"
	"sync"
	"unsafe"
)

// A Future represents a value (or error) to be available at some later
// time. Asynchronous FDB API functions return one of the types that implement
// the Future interface. All Future types additionally implement Get and MustGet
// methods with different return types. Calling BlockUntilReady, Get or MustGet
// will block the calling goroutine until the Future is ready.
type Future interface {
	// BlockUntilReady blocks the calling goroutine until the future is ready. A
	// future becomes ready either when it receives a value of its enclosed type
	// (if any) or is set to an error state.
	BlockUntilReady()

	// IsReady returns true if the future is ready, and false otherwise, without
	// blocking. A future is ready either when has received a value of its
	// enclosed type (if any) or has been set to an error state.
	IsReady() bool

	// Cancel cancels a future and its associated asynchronous operation. If
	// called before the future becomes ready, attempts to access the future
	// will return an error. Cancel has no effect if the future is already
	// ready.
	//
	// Note that even if a future is not ready, the associated asynchronous
	// operation may already have completed and be unable to be cancelled.
	Cancel()
}

type future struct {
	ptr *C.FDBFuture
}

func newFuture(ptr *C.FDBFuture) *future {
	f := &future{ptr}
	runtime.SetFinalizer(f, func(f *future) { C.fdb_future_destroy(f.ptr) })
	return f
}

func fdb_future_block_until_ready(f *C.FDBFuture) *chan struct{} {
	if C.fdb_future_is_ready(f) != 0 {
		return nil
	}

	ch := make(chan struct{}, 1)
	C.go_set_callback(unsafe.Pointer(f), unsafe.Pointer(&ch))
	<-ch
	return &ch
}

func (f future) BlockUntilReady() {
	fdb_future_block_until_ready(f.ptr)
}

func (f future) IsReady() bool {
	return C.fdb_future_is_ready(f.ptr) != 0
}

func (f future) Cancel() {
	C.fdb_future_cancel(f.ptr)
}

// FutureByteSlice represents the asynchronous result of a function that returns
// a value from a database. FutureByteSlice is a lightweight object that may be
// efficiently copied, and is safe for concurrent use by multiple goroutines.
type FutureByteSlice interface {
	// Get returns a database value (or nil if there is no value), or an error
	// if the asynchronous operation associated with this future did not
	// successfully complete. The current goroutine will be blocked until the
	// future is ready.
	Get() ([]byte, error)

	// MustGet returns a database value (or nil if there is no value), or panics
	// if the asynchronous operation associated with this future did not
	// successfully complete. The current goroutine will be blocked until the
	// future is ready.
	MustGet() []byte

	Future
}

type futureByteSlice struct {
	*future
	v []byte
	e error
	o sync.Once
}

func (f *futureByteSlice) Get() ([]byte, error) {
	f.o.Do(func() {
		var present C.fdb_bool_t
		var value *C.uint8_t
		var length C.int

		f.BlockUntilReady()

		if err := C.fdb_future_get_value(f.ptr, &present, &value, &length); err != 0 {
			f.e = Error{int(err)}
		} else {
			if present != 0 {
				f.v = C.GoBytes(unsafe.Pointer(value), length)
			}
		}

		C.fdb_future_release_memory(f.ptr)
	})

	return f.v, f.e
}

func (f *futureByteSlice) MustGet() []byte {
	val, err := f.Get()
	if err != nil {
		panic(err)
	}
	return val
}

// FutureKey represents the asynchronous result of a function that returns a key
// from a database. FutureKey is a lightweight object that may be efficiently
// copied, and is safe for concurrent use by multiple goroutines.
type FutureKey interface {
	// Get returns a database key or an error if the asynchronous operation
	// associated with this future did not successfully complete. The current
	// goroutine will be blocked until the future is ready.
	Get() (Key, error)

	// MustGet returns a database key, or panics if the asynchronous operation
	// associated with this future did not successfully complete. The current
	// goroutine will be blocked until the future is ready.
	MustGet() Key

	Future
}

type futureKey struct {
	*future
	k Key
	e error
	o sync.Once
}

func (f *futureKey) Get() (Key, error) {
	f.o.Do(func() {
		var value *C.uint8_t
		var length C.int

		f.BlockUntilReady()

		if err := C.fdb_future_get_key(f.ptr, &value, &length); err != 0 {
			f.e = Error{int(err)}
		} else {
			f.k = C.GoBytes(unsafe.Pointer(value), length)
		}

		C.fdb_future_release_memory(f.ptr)
	})

	return f.k, f.e
}

func (f *futureKey) MustGet() Key {
	val, err := f.Get()
	if err != nil {
		panic(err)
	}
	return val
}

// FutureNil represents the asynchronous result of a function that has no return
// value. FutureNil is a lightweight object that may be efficiently copied, and
// is safe for concurrent use by multiple goroutines.
type FutureNil interface {
	// Get returns an error if the asynchronous operation associated with this
	// future did not successfully complete. The current goroutine will be
	// blocked until the future is ready.
	Get() error

	// MustGet panics if the asynchronous operation associated with this future
	// did not successfully complete. The current goroutine will be blocked
	// until the future is ready.
	MustGet()

	Future
}

type futureNil struct {
	*future
}

func (f futureNil) Get() error {
	f.BlockUntilReady()
	if err := C.fdb_future_get_error(f.ptr); err != 0 {
		return Error{int(err)}
	}

	return nil
}

func (f futureNil) MustGet() {
	if err := f.Get(); err != nil {
		panic(err)
	}
}

type futureKeyValueArray struct {
	*future
}

func stringRefToSlice(ptr unsafe.Pointer) []byte {
	size := *((*C.int)(unsafe.Pointer(uintptr(ptr) + 8)))

	if size == 0 {
		return []byte{}
	}

	src := unsafe.Pointer(*(**C.uint8_t)(unsafe.Pointer(ptr)))

	return C.GoBytes(src, size)
}

func (f futureKeyValueArray) Get() ([]KeyValue, bool, error) {
	f.BlockUntilReady()

	var kvs *C.void
	var count C.int
	var more C.fdb_bool_t

	if err := C.fdb_future_get_keyvalue_array(f.ptr, (**C.FDBKeyValue)(unsafe.Pointer(&kvs)), &count, &more); err != 0 {
		return nil, false, Error{int(err)}
	}

	ret := make([]KeyValue, int(count))

	for i := 0; i < int(count); i++ {
		kvptr := unsafe.Pointer(uintptr(unsafe.Pointer(kvs)) + uintptr(i*24))
		vptr := unsafe.Pointer(uintptr(unsafe.Pointer(kvs)) + uintptr(i*24+12))

		ret[i].Key = stringRefToSlice(kvptr)
		ret[i].Value = stringRefToSlice(vptr)
	}

	return ret, (more != 0), nil
}

// FutureInt64 represents the asynchronous result of a function that returns a
// database version. FutureInt64 is a lightweight object that may be efficiently
// copied, and is safe for concurrent use by multiple goroutines.
type FutureInt64 interface {
	// Get returns a database version or an error if the asynchronous operation
	// associated with this future did not successfully complete. The current
	// goroutine will be blocked until the future is ready.
	Get() (int64, error)

	// MustGet returns a database version, or panics if the asynchronous
	// operation associated with this future did not successfully complete. The
	// current goroutine will be blocked until the future is ready.
	MustGet() int64

	Future
}

type futureInt64 struct {
	*future
}

func (f futureInt64) Get() (int64, error) {
	f.BlockUntilReady()

	var ver C.int64_t
	if err := C.fdb_future_get_version(f.ptr, &ver); err != 0 {
		return 0, Error{int(err)}
	}
	return int64(ver), nil
}

func (f futureInt64) MustGet() int64 {
	val, err := f.Get()
	if err != nil {
		panic(err)
	}
	return val
}

// FutureStringSlice represents the asynchronous result of a function that
// returns a slice of strings. FutureStringSlice is a lightweight object that
// may be efficiently copied, and is safe for concurrent use by multiple
// goroutines.
type FutureStringSlice interface {
	// Get returns a slice of strings or an error if the asynchronous operation
	// associated with this future did not successfully complete. The current
	// goroutine will be blocked until the future is ready.
	Get() ([]string, error)

	// MustGet returns a slice of strings or panics if the asynchronous
	// operation associated with this future did not successfully complete. The
	// current goroutine will be blocked until the future is ready.
	MustGet() []string

	Future
}

type futureStringSlice struct {
	*future
}

func (f futureStringSlice) Get() ([]string, error) {
	f.BlockUntilReady()

	var strings **C.char
	var count C.int

	if err := C.fdb_future_get_string_array(f.ptr, (***C.char)(unsafe.Pointer(&strings)), &count); err != 0 {
		return nil, Error{int(err)}
	}

	ret := make([]string, int(count))

	for i := 0; i < int(count); i++ {
		ret[i] = C.GoString((*C.char)(*(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(strings)) + uintptr(i*8)))))
	}

	return ret, nil
}

func (f futureStringSlice) MustGet() []string {
	val, err := f.Get()
	if err != nil {
		panic(err)
	}
	return val
}
