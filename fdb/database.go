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
 #define FDB_API_VERSION 200
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
// usually created and committed automatically by the (Database).Transact
// method.
type Database struct {
	*database
}

type database struct {
	ptr *C.FDBDatabase
}

// DatabaseOptions is a handle with which to set options that affect a Database
// object. A DatabaseOptions instance should be obtained with the
// (Database).Options method.
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
// preferable to use the (Database).Transact method, which handles
// automatically creating and committing a transaction with appropriate retry
// behavior.
func (d Database) CreateTransaction() (Transaction, error) {
	var outt *C.FDBTransaction

	if err := C.fdb_database_create_transaction(d.ptr, &outt); err != 0 {
		return Transaction{}, Error{int(err)}
	}

	t := &transaction{outt, d}
	runtime.SetFinalizer(t, (*transaction).destroy)

	return Transaction{t}, nil
}

func retryable(wrapped func() (interface{}, error), onError func(Error) FutureNil) (ret interface{}, e error) {
	for {
		ret, e = wrapped()

		/* No error means success! */
		if e == nil {
			return
		}

		ep, ok := e.(Error)
		if ok {
			e = onError(ep).Get()
		}

		/* If OnError returns an error, then it's not
		/* retryable; otherwise take another pass at things */
		if e != nil {
			return
		}
	}
}

// Transact runs a caller-provided function inside a retry loop, providing it
// with a newly created Transaction. After the function returns, the Transaction
// will be committed automatically. Any error during execution of the function
// (by panic or return) or the commit will cause the function and commit to be
// retried or, if fatal, return the error to the caller.
//
// When working with Future objects in a transactional function, you may either
// explicity check and return error values using Get, or call MustGet. Transact
// will recover a panicked Error and either retry the transaction or return the
// error.
//
// Do not return Future objects from the function provided to Transact. The
// Transaction created by Transact may be finalized at any point after Transact
// returns, resulting in the cancellation of any outstanding
// reads. Additionally, any errors returned or panicked by the Future will no
// longer be able to trigger a retry of the caller-provided function.
//
// See the Transactor interface for an example of using Transact with
// Transaction and Database objects.
func (d Database) Transact(f func(Transaction) (interface{}, error)) (interface{}, error) {
	tr, e := d.CreateTransaction()
	/* Any error here is non-retryable */
	if e != nil {
		return nil, e
	}

	wrapped := func() (ret interface{}, e error) {
		defer panicToError(&e)

		ret, e = f(tr)

		if e == nil {
			e = tr.Commit().Get()
		}

		return
	}

	return retryable(wrapped, tr.OnError)
}

// ReadTransact runs a caller-provided function inside a retry loop, providing
// it with a newly created Transaction (as a ReadTransaction). Any error during
// execution of the function (by panic or return) will cause the function to be
// retried or, if fatal, return the error to the caller.
//
// When working with Future objects in a read-only transactional function, you
// may either explicity check and return error values using Get, or call
// MustGet. ReadTransact will recover a panicked Error and either retry the
// transaction or return the error.
//
// Do not return Future objects from the function provided to ReadTransact. The
// Transaction created by ReadTransact may be finalized at any point after
// ReadTransact returns, resulting in the cancellation of any outstanding
// reads. Additionally, any errors returned or panicked by the Future will no
// longer be able to trigger a retry of the caller-provided function.
//
// See the ReadTransactor interface for an example of using ReadTransact with
// Transaction, Snapshot and Database objects.
func (d Database) ReadTransact(f func(ReadTransaction) (interface{}, error)) (interface{}, error) {
	tr, e := d.CreateTransaction()
	/* Any error here is non-retryable */
	if e != nil {
		return nil, e
	}

	wrapped := func() (ret interface{}, e error) {
		defer panicToError(&e)

		ret, e = f(tr)

		if e == nil {
			e = tr.Commit().Get()
		}

		return
	}

	return retryable(wrapped, tr.OnError)
}

// Options returns a DatabaseOptions instance suitable for setting options
// specific to this database.
func (d Database) Options() DatabaseOptions {
	return DatabaseOptions{d.database}
}

// LocalityGetBoundaryKeys returns a slice of keys that fall within the provided
// range. Each key is located at the start of a contiguous range stored on a
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

	bk, ek := er.FDBRangeKeys()
	ffer := KeyRange{append(Key("\xFF/keyServers/"), bk.FDBKey()...), append(Key("\xFF/keyServers/"), ek.FDBKey()...)}

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
