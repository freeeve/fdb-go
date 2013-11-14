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

package fdb_test

import (
	"fmt"
	"github.com/FoundationDB/fdb-go/fdb"
)

func setOne(t fdb.Transactor, key []byte, value []byte) {
	fmt.Printf("setOne got:  %T\n", t)
	t.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// We don't actually call tr.Set here to avoid mutating a real database.
		// tr.Set(key, value)
		return nil, nil
	})
}

func setMany(t fdb.Transactor, value []byte, keys ...[]byte) {
	fmt.Printf("setMany got: %T\n", t)
	t.Transact(func(tr fdb.Transaction) (interface{}, error) {
		for _, key := range(keys) {
			setOne(tr, key, value)
		}
		return nil, nil
	})
}

func ExampleTransactor() {
	_ = fdb.APIVersion(200)
	db, _ := fdb.OpenDefault()

	fmt.Printf("Calling setOne with a database:\n")
	setOne(db, []byte("foo"), []byte("bar"))
	fmt.Printf("\nCalling setMany with a database:\n")
	setMany(db, []byte("bar"), []byte("foo1"), []byte("foo2"), []byte("foo3"))

	// Output:
	// Calling setOne with a database:
	// setOne got:  fdb.Database
	//
	// Calling setMany with a database:
	// setMany got: fdb.Database
	// setOne got:  fdb.Transaction
	// setOne got:  fdb.Transaction
	// setOne got:  fdb.Transaction
}
