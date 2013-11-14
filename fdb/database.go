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
 #define FDB_API_VERSION 100
 #include <foundationdb/fdb_c.h>
*/
import "C"

import (
	"runtime"
)

// Database is a handle to a FoundationDB database. Database is a lightweight
// object that may be efficiently copied, and is safe for concurrent use by
// multiple goroutines.
//
// Although Database provides convenience methods for reading and writing data,
// modifications to a database are usually made via transactions, which are
// usually created and committed automatically by the (Database).Transact()
// method.
type Database struct {
	*database
}

type database struct {
	ptr *C.FDBDatabase
}

// DatabaseOptions is a handle with which to set options that affect a Database
// object. A DatabaseOptions instance should be obtained with the
// (Database).Options() method.
type DatabaseOptions struct {
	d *database
}

func (opt DatabaseOptions) setOpt(code int, param []byte) error {
	return setOpt(func(p *C.uint8_t, pl C.int) C.fdb_error_t {
		return C.fdb_database_set_option(opt.d.ptr, C.FDBDatabaseOption(code), p, pl)
	}, param)
}

func (d *database) destroy() {
	C.fdb_database_destroy(d.ptr)
}

// CreateTransaction returns a new FoundationDB transaction. It is generally
// preferable to use the (Database).Transact() method, which handles
// automatically creating and committing a transaction with appropriate retry
// behavior.
func (d Database) CreateTransaction() (Transaction, error) {
	var outt *C.FDBTransaction

	if err := C.fdb_database_create_transaction(d.ptr, &outt); err != 0 {
		return Transaction{}, FDBError{int(err)}
	}

	t := &transaction{outt, d}
	runtime.SetFinalizer(t, (*transaction).destroy)

	return Transaction{t}, nil
}

// Transact runs a caller-provided function inside a retry loop, providing it
// with a newly created transaction. After the function returns, the transaction
// will be committed automatically. Any error during execution of the caller's
// function (by panic or return) or the commit will cause the entire transaction
// to be retried or, if fatal, return the error to the caller.
//
// When working with fdb Future objects in a transactional fucntion, you may
// either explicity check and return error values from (Future).GetWithError(),
// or call (Future).GetOrPanic(). Transact will recover a panicked fdb.Error and
// either retry the transaction or return the error.
//
// See the Transactor interface for an example of using Transact with
// Transaction and Database objects.
func (d Database) Transact(f func(tr Transaction) (interface{}, error)) (ret interface{}, e error) {
	tr, e := d.CreateTransaction()
	/* Any error here is non-retryable */
	if e != nil {
		return
	}

	wrapped := func() {
		defer func() {
			if r := recover(); r != nil {
				switch r := r.(type) {
				case FDBError:
					e = r
				default:
					panic(r)
				}
			}
		}()

		ret, e = f(tr)

		if e != nil {
			return
		}

		e = tr.Commit().GetWithError()
	}

	for {
		wrapped()

		/* No error means success! */
		if e == nil {
			return
		}

		switch ep := e.(type) {
		case FDBError:
			e = tr.OnError(ep).GetWithError()
		}

		/* If OnError returns an error, then it's not
		/* retryable; otherwise take another pass at things */
		if e != nil {
			return
		}
	}
}

// Get returns the value associated with the specified key (or nil if the key
// does not exist). This read blocks the current goroutine until complete.
func (d Database) Get(key KeyConvertible) ([]byte, error) {
	v, e := d.Transact(func (tr Transaction) (interface{}, error) {
		return tr.Get(key).GetOrPanic(), nil
	})
	if e != nil {
		return nil, e
	}
	return v.([]byte), nil
}

// GetKey returns the key referenced by the specified key selector. This read
// blocks the current goroutine until complete.
func (d Database) GetKey(sel Selectable) (Key, error) {
	v, e := d.Transact(func (tr Transaction) (interface{}, error) {
		return tr.GetKey(sel).GetOrPanic(), nil
	})
	if e != nil {
		return nil, e
	}
	return v.(Key), nil
}

// GetRange returns a slice of KeyValue objects kv such that beginKey <= kv.Key
// < endKey, ordered by kv.Key. beginKey and endKey are the keys described by
// the key selectors r.BeginKeySelector() and r.EndKeySelector(). This read
// blocks the current goroutine until complete.
func (d Database) GetRange(r Range, options RangeOptions) ([]KeyValue, error) {
	v, e := d.Transact(func (tr Transaction) (interface{}, error) {
		return tr.GetRange(r, options).GetSliceOrPanic(), nil
	})
	if e != nil {
		return nil, e
	}
	return v.([]KeyValue), nil
}

// Set associates the specified key and value, overwriting any previous value
// associated with key. This change will be committed immediately and blocks the
// current goroutine until complete.
func (d Database) Set(key KeyConvertible, value []byte) error {
	_, e := d.Transact(func (tr Transaction) (interface{}, error) {
		tr.Set(key, value)
		return nil, nil
	})
	if e != nil {
		return e
	}
	return nil
}

// Clear removes the specified key (and any associated value), if it
// exists. This change will be committed immediately and blocks the current
// goroutine until complete.
func (d Database) Clear(key KeyConvertible) error {
	_, e := d.Transact(func (tr Transaction) (interface{}, error) {
		tr.Clear(key)
		return nil, nil
	})
	if e != nil {
		return e
	}
	return nil
}

// ClearRange removes all keys k such that er.BeginKey() <= k < er.EndKey(), and
// their associated values. This change will be committed immediately and blocks
// the current goroutine until complete.
func (d Database) ClearRange(er ExactRange) error {
	_, e := d.Transact(func (tr Transaction) (interface{}, error) {
		tr.ClearRange(er)
		return nil, nil
	})
	if e != nil {
		return e
	}
	return nil
}

// GetAndWatch returns the value associated with the specified key (or nil if
// the key does not exist), along with a future that will become ready when the
// value associated with the key changes. This read blocks the current goroutine
// until complete.
func (d Database) GetAndWatch(key KeyConvertible) ([]byte, FutureNil, error) {
	r, e := d.Transact(func (tr Transaction) (interface{}, error) {
		v := tr.Get(key).GetOrPanic()
		w := tr.Watch(key)
		return struct{value []byte; watch FutureNil}{v, w}, nil
		return nil, nil
	})
	if e != nil {
		return nil, FutureNil{}, e
	}
	ret := r.(struct{value []byte; watch FutureNil})
	return ret.value, ret.watch, nil
}

// SetAndWatch associates the specified key and value, overwriting any previous
// value associated with key, and returns a future that will become ready when
// the value associated with the key changes. This change will be committed
// immediately and blocks the current goroutine until complete.
func (d Database) SetAndWatch(key KeyConvertible, value []byte) (FutureNil, error) {
	r, e := d.Transact(func (tr Transaction) (interface{}, error) {
		tr.Set(key, value)
		return tr.Watch(key), nil
	})
	if e != nil {
		return FutureNil{}, e
	}
	return r.(FutureNil), nil
}

// Clear removes the specified key (and any associated value), if it exists, and
// returns a future that will become ready when the value associated with the
// key changes. This change will be committed immediately and blocks the current
// goroutine until complete.
func (d Database) ClearAndWatch(key KeyConvertible) (FutureNil, error) {
	r, e := d.Transact(func (tr Transaction) (interface{}, error) {
		tr.Clear(key)
		return tr.Watch(key), nil
	})
	if e != nil {
		return FutureNil{}, e
	}
	return r.(FutureNil), nil
}

// Options returns a DatabaseOptions instance suitable for setting options
// specific to this database.
func (d Database) Options() DatabaseOptions {
	return DatabaseOptions{d.database}
}

// LocalityGetAddressesForKey returns the public network addresses of each of
// the storage servers responsible for storing key and its associated
// value. This read blocks the current goroutine until complete.
func (d Database) LocalityGetAddressesForKey(key KeyConvertible) ([]string, error) {
	v, e := d.Transact(func (tr Transaction) (interface{}, error) {
		return tr.LocalityGetAddressesForKey(key).GetOrPanic(), nil
	})
	if e != nil {
		return nil, e
	}
	return v.([]string), nil
}

// LocalityGetBoundaryKeys returns a slice of keys that fall within the range
// where each key is located at the start of a contiguous range stored on a
// single server.
//
// If limit is non-zero, only the first limit keys will be returned. In large
// databases, the number of boundary keys may be large. In these cases, a
// non-zero limit should be used, along with multiple calls to
// LocalityGetBoundaryKeys.
//
// If readVersion is non-zero, the boundary keys as of readVersion will be
// returned.
func (d Database) LocalityGetBoundaryKeys(er ExactRange, limit int, readVersion int64) ([]Key, error) {
	tr, e := d.CreateTransaction()
	if e != nil {
		return nil, e
	}

	if readVersion != 0 {
		tr.SetReadVersion(readVersion)
	}

	tr.Options().SetAccessSystemKeys()

	ffer := KeyRange{append(Key("\xFF/keyServers/"), er.BeginKey()...), append(Key("\xFF/keyServers/"), er.EndKey()...)}

	kvs, e := tr.Snapshot().GetRange(ffer, RangeOptions{Limit: limit}).GetSliceWithError()
	if e != nil {
		return nil, e
	}

	size := len(kvs)
	if limit != 0 && limit < size {
		size = limit
	}

	boundaries := make([]Key, size)

	for i := 0; i < size; i++ {
		boundaries[i] = kvs[i].Key[13:]
	}

	return boundaries, nil
}
