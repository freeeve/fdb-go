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
	"github.com/FoundationDB/fdb-go/fdb"
	"fmt"
)

func ExampleOpenDefault() {
	var e error

	e = fdb.APIVersion(200)
	if e != nil {
		fmt.Printf("Unable to set API version: %v\n", e)
		return
	}

	// OpenDefault opens the database described by the platform-specific default
	// cluster file and the database name []byte("DB").
	db, e := fdb.OpenDefault()
	if e != nil {
		fmt.Printf("Unable to open default database: %v\n", e)
		return
	}

	_ = db
}

func ExampleTransactor() {
	fdb.MustAPIVersion(200)
	db := fdb.MustOpenDefault()

	setOne := func(t fdb.Transactor, key fdb.Key, value []byte) error {
		fmt.Printf("setOne called with:  %T\n", t)
		_, e := t.Transact(func(tr fdb.Transaction) (interface{}, error) {
			// We don't actually call tr.Set here to avoid mutating a real database.
			// tr.Set(key, value)
			return nil, nil
		})
		return e
	}

	setMany := func(t fdb.Transactor, value []byte, keys ...fdb.Key) error {
		fmt.Printf("setMany called with: %T\n", t)
		_, e := t.Transact(func(tr fdb.Transaction) (interface{}, error) {
			for _, key := range(keys) {
				setOne(tr, key, value)
			}
			return nil, nil
		})
		return e
	}

	var e error

	fmt.Println("Calling setOne with a database:")
	e = setOne(db, []byte("foo"), []byte("bar"))
	if e != nil {
		fmt.Println(e)
		return
	}
	fmt.Println("\nCalling setMany with a database:")
	e = setMany(db, []byte("bar"), fdb.Key("foo1"), fdb.Key("foo2"), fdb.Key("foo3"))
	if e != nil {
		fmt.Println(e)
		return
	}

	// Output:
	// Calling setOne with a database:
	// setOne called with:  fdb.Database
	//
	// Calling setMany with a database:
	// setMany called with: fdb.Database
	// setOne called with:  fdb.Transaction
	// setOne called with:  fdb.Transaction
	// setOne called with:  fdb.Transaction
}

func ExampleReadTransactor() {
	fdb.MustAPIVersion(200)
	db := fdb.MustOpenDefault()

	getOne := func(rt fdb.ReadTransactor, key fdb.Key) ([]byte, error) {
		fmt.Printf("getOne called with: %T\n", rt)
		ret, e := rt.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			return rtr.Get(key).MustGet(), nil
		})
		if e != nil {
			return nil, e
		}
		return ret.([]byte), nil
	}

	getTwo := func(rt fdb.ReadTransactor, key1, key2 fdb.Key) ([][]byte, error) {
		fmt.Printf("getTwo called with: %T\n", rt)
		ret, e := rt.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			r1, _ := getOne(rtr, key1)
			r2, _ := getOne(rtr.Snapshot(), key2)
			return [][]byte{r1, r2}, nil
		})
		if e != nil {
			return nil, e
		}
		return ret.([][]byte), nil
	}

	var e error

	fmt.Println("Calling getOne with a database:")
	_, e = getOne(db, fdb.Key("foo"))
	if e != nil {
		fmt.Println(e)
		return
	}
	fmt.Println("\nCalling getTwo with a database:")
	_, e = getTwo(db, fdb.Key("foo"), fdb.Key("bar"))
	if e != nil {
		fmt.Println(e)
		return
	}

	// Output:
	// Calling getOne with a database:
	// getOne called with: fdb.Database
	//
	// Calling getTwo with a database:
	// getTwo called with: fdb.Database
	// getOne called with: fdb.Transaction
	// getOne called with: fdb.Snapshot
}

func ExamplePrefixRange() {
	fdb.MustAPIVersion(200)
	db := fdb.MustOpenDefault()

	tr, e := db.CreateTransaction()
	if e != nil {
		fmt.Printf("Unable to create transaction: %v\n", e)
		return
	}

	// Clear and initialize data in this transaction. In examples we do not
	// commit transactions to avoid mutating a real database.
	tr.ClearRange(fdb.KeyRange{fdb.Key(""), fdb.Key{0xFF}})
	tr.Set(fdb.Key("alpha"), []byte("1"))
	tr.Set(fdb.Key("alphabetA"), []byte("2"))
	tr.Set(fdb.Key("alphabetB"), []byte("3"))
	tr.Set(fdb.Key("alphabetize"), []byte("4"))
	tr.Set(fdb.Key("beta"), []byte("5"))

	// Construct the range of all keys beginning with "alphabet". It is safe to
	// ignore the error return from PrefixRange unless the provided prefix might
	// consist entirely of zero or more 0xFF bytes.
	pr, _ := fdb.PrefixRange([]byte("alphabet"))

	// Read and process the range
	kvs, e := tr.GetRange(pr, fdb.RangeOptions{}).GetSliceWithError()
	if e != nil {
		fmt.Printf("Unable to read range: %v\n", e)
	}
	for _, kv := range kvs {
		fmt.Printf("%s: %s\n", string(kv.Key), string(kv.Value))
	}

	// Output:
	// alphabetA: 2
	// alphabetB: 3
	// alphabetize: 4
}

func ExampleRangeIterator() {
	fdb.MustAPIVersion(200)
	db := fdb.MustOpenDefault()

	tr, e := db.CreateTransaction()
	if e != nil {
		fmt.Printf("Unable to create transaction: %v\n", e)
		return
	}

	// Clear and initialize data in this transaction. In examples we do not
	// commit transactions to avoid mutating a real database.
	tr.ClearRange(fdb.KeyRange{fdb.Key(""), fdb.Key{0xFF}})
	tr.Set(fdb.Key("apple"), []byte("foo"))
	tr.Set(fdb.Key("cherry"), []byte("baz"))
	tr.Set(fdb.Key("banana"), []byte("bar"))

	rr := tr.GetRange(fdb.KeyRange{fdb.Key(""), fdb.Key{0xFF}}, fdb.RangeOptions{})
	ri := rr.Iterator()

	// Advance will return true until the iterator is exhausted
	for ri.Advance() {
		kv, e := ri.Get()
		if e != nil {
			fmt.Printf("Unable to read next value: %v\n", e)
			return
		}
		fmt.Printf("%s is %s\n", kv.Key, kv.Value)
	}

	// Output:
	// apple is foo
	// banana is bar
	// cherry is baz
}
