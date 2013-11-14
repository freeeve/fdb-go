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
 #define FDB_API_VERSION 100
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
	"unsafe"
)

type future struct {
	ptr *C.FDBFuture
}

func (f *future) destroy() {
	C.fdb_future_destroy(f.ptr)
}

func fdb_future_block_until_ready(f *C.FDBFuture) {
	if C.fdb_future_is_ready(f) != 0 {
		return
	}

	ch := make(chan struct{}, 1)
	C.go_set_callback(unsafe.Pointer(f), unsafe.Pointer(&ch))
	<-ch
}

// BlockUntilReady blocks the calling goroutine until the future is ready. A
// future becomes ready either when it receives a value of its enclosed type (if
// any) or is set to an error state.
func (f *future) BlockUntilReady() {
	fdb_future_block_until_ready(f.ptr)
}

// IsReady returns true if the future is ready, and false otherwise, without
// blocking. A future is ready either when has received a value of its enclosed
// type (if any) or has been set to an error state.
func (f *future) IsReady() bool {
	return C.fdb_future_is_ready(f.ptr) != 0
}

// Cancel cancels a future and its associated asynchronous operation. If called
// before the future becomes ready, attempts to access the future will return an
// error. Cancel has no effect if the future is already ready.
//
// Note that even if a future is not ready, the associated asynchronous
// operation may already have completed and be unable to be cancelled.
func (f *future) Cancel() {
	C.fdb_future_cancel(f.ptr)
}

// FutureValue represents the asynchronous result of a function that returns a
// value from a database. FutureValue is a lightweight object that may be
// efficiently copied, and is safe for concurrent use by multiple goroutines.
type FutureValue struct {
	*futureValue
}

type futureValue struct {
	*future
	v   []byte
	set bool
}

// These are implemented again so that they show up in the generated
// documentation!

// BlockUntilReady blocks the calling goroutine until the future is ready. A
// future becomes ready either when it receives a value of its enclosed type (if
// any) or is set to an error state.
func (f *futureValue) BlockUntilReady() {
	fdb_future_block_until_ready(f.ptr)
}

// IsReady returns true if the future is ready, and false otherwise, without
// blocking. A future is ready either when has received a value of its enclosed
// type (if any) or has been set to an error state.
func (f *futureValue) IsReady() bool {
	return C.fdb_future_is_ready(f.ptr) != 0
}

// Cancel cancels a future and its associated asynchronous operation. If called
// before the future becomes ready, attempts to access the future will return an
// error. Cancel has no effect if the future is already ready.
//
// Note that even if a future is not ready, the associated asynchronous
// operation may already have completed and be unable to be cancelled.
func (f *futureValue) Cancel() {
	C.fdb_future_cancel(f.ptr)
}

// GetWithError returns a database value (or nil if there is no value), or an
// error if the asynchronous operation associated with this future did not
// successfully complete. The current goroutine will be blocked until the future
// is ready.
func (f FutureValue) GetWithError() ([]byte, error) {
	if f.set {
		return f.v, nil
	}

	var present C.fdb_bool_t
	var value *C.uint8_t
	var length C.int

	fdb_future_block_until_ready(f.ptr)
	if err := C.fdb_future_get_value(f.ptr, &present, &value, &length); err != 0 {
		if err == 2017 {
			return f.v, nil
		} else {
			return nil, Error{int(err)}
		}
	}

	if present != 0 {
		f.v = C.GoBytes(unsafe.Pointer(value), length)
	}
	f.set = true

	C.fdb_future_release_memory(f.ptr)

	return f.v, nil
}

// GetOrPanic returns a database value (or nil if there is no value), or panics
// if the asynchronous operation associated with this future did not
// successfully complete. The current goroutine will be blocked until the future
// is ready.
func (f FutureValue) GetOrPanic() []byte {
	val, err := f.GetWithError()
	if err != nil {
		panic(err)
	}
	return val
}

// FutureKey represents the asynchronous result of a function that returns a key
// from a database. FutureKey is a lightweight object that may be efficiently
// copied, and is safe for concurrent use by multiple goroutines.
type FutureKey struct {
	*futureKey
}

type futureKey struct {
	*future
	k Key
}

// These are implemented again so that they show up in the generated
// documentation!

// BlockUntilReady blocks the calling goroutine until the future is
// ready. A future becomes ready either when it receives a value of
// its enclosed type (if any) or is set to an error state.
func (f *futureKey) BlockUntilReady() {
	fdb_future_block_until_ready(f.ptr)
}

// IsReady returns true if the future is ready, and false otherwise,
// without blocking. A future is ready either when has received a
// value of its enclosed type (if any) or has been set to an error
// state.
func (f *futureKey) IsReady() bool {
	return C.fdb_future_is_ready(f.ptr) != 0
}

// Cancel cancels a future and its associated asynchronous
// operation. If called before the future becomes ready, attempts to
// access the future will return an error. Cancel has no effect if the
// future is already ready.
//
// Note that even if a future is not ready, the associated
// asynchronous operation may already have completed and be unable to
// be cancelled.
func (f *futureKey) Cancel() {
	C.fdb_future_cancel(f.ptr)
}

// GetWithError returns a database key or an error if the asynchronous operation
// associated with this future did not successfully complete. The current
// goroutine will be blocked until the future is ready.
func (f FutureKey) GetWithError() (Key, error) {
	if f.k != nil {
		return f.k, nil
	}

	var value *C.uint8_t
	var length C.int

	fdb_future_block_until_ready(f.ptr)
	if err := C.fdb_future_get_key(f.ptr, &value, &length); err != 0 {
		if err == 2017 {
			return f.k, nil
		} else {
			return nil, Error{int(err)}
		}
	}

	f.k = C.GoBytes(unsafe.Pointer(value), length)

	C.fdb_future_release_memory(f.ptr)

	return f.k, nil
}

// GetOrPanic returns a database key, or panics if the asynchronous operation
// associated with this future did not successfully complete. The current
// goroutine will be blocked until the future is ready.
func (f FutureKey) GetOrPanic() Key {
	val, err := f.GetWithError()
	if err != nil {
		panic(err)
	}
	return val
}

// FutureNil represents the asynchronous result of a function that has no return
// value. FutureNil is a lightweight object that may be efficiently copied, and
// is safe for concurrent use by multiple goroutines.
type FutureNil struct {
	*future
}

// GetWithError returns an error if the asynchronous operation associated with
// this future did not successfully complete. The current goroutine will be
// blocked until the future is ready.
func (f FutureNil) GetWithError() error {
	fdb_future_block_until_ready(f.ptr)
	if err := C.fdb_future_get_error(f.ptr); err != 0 {
		return Error{int(err)}
	}

	return nil
}

// GetOrPanic panics if the asynchronous operation associated with this future
// did not successfully complete. The current goroutine will be blocked until
// the future is ready.
func (f FutureNil) GetOrPanic() {
	if err := f.GetWithError(); err != nil {
		panic(err)
	}
}

type futureKeyValueArray struct {
	*future
}

func stringRefToSlice(ptr uintptr) []byte {
	size := *((*C.int)(unsafe.Pointer(ptr+8)))

	if size == 0 {
		return []byte{}
	}

	src := unsafe.Pointer(*(**C.uint8_t)(unsafe.Pointer(ptr)))

	return C.GoBytes(src, size)
}

func (f *futureKeyValueArray) GetWithError() ([]KeyValue, bool, error) {
	fdb_future_block_until_ready(f.ptr)

	var kvs *C.void
	var count C.int
	var more C.fdb_bool_t

	if err := C.fdb_future_get_keyvalue_array(f.ptr, (**C.FDBKeyValue)(unsafe.Pointer(&kvs)), &count, &more); err != 0 {
		return nil, false, Error{int(err)}
	}

	ret := make([]KeyValue, int(count))

	for i := 0; i < int(count); i++ {
		kvptr := uintptr(unsafe.Pointer(kvs)) + uintptr(i * 24)

		ret[i].Key = stringRefToSlice(kvptr)
		ret[i].Value = stringRefToSlice(kvptr + 12)

	}

 	return ret, (more != 0), nil
}

// FutureVersion represents the asynchronous result of a function that returns a
// database version. FutureVersion is a lightweight object that may be
// efficiently copied, and is safe for concurrent use by multiple goroutines.
type FutureVersion struct {
	*future
}

// GetWithError returns a database version or an error if the asynchronous
// operation associated with this future did not successfully complete. The
// current goroutine will be blocked until the future is ready.
func (f FutureVersion) GetWithError() (int64, error) {
	fdb_future_block_until_ready(f.ptr)

	var ver C.int64_t
	if err := C.fdb_future_get_version(f.ptr, &ver); err != 0 {
		return 0, Error{int(err)}
	}
	return int64(ver), nil
}

// GetOrPanic returns a database version, or panics if the asynchronous
// operation associated with this future did not successfully complete. The
// current goroutine will be blocked until the future is ready.
func (f FutureVersion) GetOrPanic() int64 {
	val, err := f.GetWithError()
	if err != nil {
		panic(err)
	}
	return val
}

type FutureStringArray struct {
	*future
}

func (f FutureStringArray) GetWithError() ([]string, error) {
	fdb_future_block_until_ready(f.ptr)

	var strings **C.char
	var count C.int

	if err := C.fdb_future_get_string_array(f.ptr, (***C.char)(unsafe.Pointer(&strings)), &count); err != 0 {
		return nil, Error{int(err)}
	}

	ret := make([]string, int(count))

	for i := 0; i < int(count); i++ {
		ret[i] = C.GoString((*C.char)(*(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(strings))+uintptr(i*8)))))
	}

	return ret, nil
}

func (f FutureStringArray) GetOrPanic() []string {
	val, err := f.GetWithError()
	if err != nil {
		panic(err)
	}
	return val
}
