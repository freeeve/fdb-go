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

/*
Package fdb provides an interface to FoundationDB databases (version 2.0).

To build and run programs using this package, you must have an installed copy of
the FoundationDB client libraries (version 2.0.0 or later), available for Linux,
Windows and OS X at https://foundationdb.com/get.

This documentation specifically applies to the FoundationDB Go binding. For more
extensive guidance to programming with FoundationDB, as well as API
documentation for the other FoundationDB interfaces, please see
https://foundationdb.com/documentation.

Basic Usage

A basic interaction with the FoundationDB API is demonstrated below:

    package main

    import (
        "github.com/FoundationDB/fdb-go/fdb"
        "log"
        "fmt"
    )

    func main() {
        var e error

        // Different API versions may expose different runtime behaviors.
        e = fdb.APIVersion(200)
        if e != nil {
            log.Fatalf("Unable to load FDB at API version 200 (%v)", e)
        }

        // Open the default database from the system cluster
        db, e := fdb.OpenDefault()
        if e != nil {
            log.Fatalf("Unable to open default FDB database (%v)", e)
        }

        // We can perform reads or writes directly on a database...
        e = db.Set(fdb.Key("hello"), []byte("world"))
        if e != nil {
            log.Fatalf("Unable to set FDB database value (%v)", e)
        }

        // or with more control inside a transaction.
        ret, e := db.Transact(func (tr fdb.Transaction) (interface{}, error) {
            orig := tr.Get(fdb.Key("hello")).GetOrPanic()
            tr.Set(fdb.Key("hello"), []byte("universe"))
            fmt.Println("Setting hello to universe...")
            return orig, nil
        })
        if e != nil {
            log.Fatalf("Unable to perform FDB transaction (%v)", e)
        }

        fmt.Printf("The original greeting was to: %s\n", string(ret.([]byte)))
    }

On Panics

Idiomatic Go code strongly frowns at panics that escape library/package
boundaries, in favor of explicitly returned errors. Idiomatic FoundationDB
client programs, however, are built around the idea of retryable
programmer-provided transactional functions. Retryable transactions can be
implemented using only error values:

    ret, e := db.Transact(func (tr Transaction) (interface{}, error) {
        // FoundationDB futures represent a value that will become available
        futureValueOne := tr.Get(fdb.Key("foo"))
        futureValueTwo := tr.Get(fdb.Key("bar"))

        // Both reads are being carried out in parallel

        // Get the first value (or any error)
        valueOne, e := futureValueOne.GetWithError()
        if e != nil {
            return nil, e
        }

        // Get the second value (or any error)
        valueTwo, e := futureValueTwo.GetWithError()
        if e != nil {
            return nil, e
        }

        // Return the two values
        return []string{valueOne, valueTwo}, nil
    })

If either read encounters an error, it will be returned to Transact(), which
will determine if the error is retryable or not (using the OnError() method of
Transaction). If the error is an FDB Error and retryable (such as a conflict
with with another transaction), then the programmer-provided function will be
run again. If the error is fatal (or not an FDB Error), then the error will be
returned to the caller of Transact().

In practice, checking for an error from every asynchronous future type in the
FoundationDB API quickly becomes frustrating. As a convenience, every Future
type also provides a method GetOrPanic(), which returns the same type and value
as GetWithError(), but exposes FoundationDB Errors via a panic rather than an
explicitly returned error. The above example may be rewritten as:

    ret, e := db.Transact(func (tr Transaction) (interface{}, error) {
        // FoundationDB futures represent a value that will become available
        futureValueOne := tr.Get(fdb.Key("foo"))
        futureValueTwo := tr.Get(fdb.Key("bar"))

        // Both reads are being carried out in parallel

        // Get the first value
        valueOne := futureValueOne.GetOrPanic()
        // Get the second value
        valueTwo := futureValueTwo.GetOrPanic()

        // Return the two values
        return []string{valueOne, valueTwo}, nil
    })

Any panic that occurs during execution of the caller-provided function will be
recovered by the Transact() method of Database. If the error is an FDB Error, it
will either result in a retry of the function or be returned by Transact(). If
the error is any other type (panics from code other than GetOrPanic()),
Transact() will re-panic the original value.

Note that the Transact() method of Transaction does not recover
panics. (Transaction).Transact() exists to allow composition of transactional
functions, i.e. calling a function that takes a Transactor from inside another
transactional function (see the Transactor example below). Any panic is assumed
to be handled by an enclosing (Database).Transact() wrapper.

Streaming Modes

When using GetRange() methods in the FoundationDB API, clients can request large
ranges of the database to iterate over. Making such a request doesn't
necessarily mean that the client will consume all of the data in the range --
sometimes the client doesn't know how far it intends to iterate in
advance. FoundationDB tries to balance latency and bandwidth by requesting data
for iteration in batches.

The Mode field of the RangeOptions struct allows a client to customize this
performance tradeoff by providing extra information about how the iterator will
be used.

The default value of Mode is StreamingModeIterator, which tries to provide a
reasonable default balance. Other streaming modes that prioritize throughput or
latency are available -- see the documented StreamingMode values for specific
options.

Atomic Operations

The FDB package provides a number of atomic operations on the Database and
Transaction objects. An atomic operation is a single database command that
carries out several logical steps: reading the value of a key, performing a
transformation on that value, and writing the result. Different atomic
operations perform different transformations. Like other database operations, an
atomic operation is used within a transaction.

For more information on atomic operations in FoundationDB, please see
https://foundationdb.com/documentation/developer-guide.html#atomic-operations. The
operands to atomic operations in this API must be provided as appropriately
encoded byte slices. To convert a Go type to a byte slice, see the binary
package.

The current atomic operation methods in this API are Add(), BitAnd(), BitOr()
and BitXor() (on the Transaction and Database objects).
*/
package fdb
