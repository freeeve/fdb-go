// FoundationDB Go Subspace Layer
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

// Package subspace provides a convenient way to use FoundationDB tuples to
// define namespaces for different categories of data. The namespace is
// specified by a prefix tuple which is prepended to all tuples packed by the
// subspace. When unpacking a key with the subspace, the prefix tuple will be
// removed from the result.
//
// As a best practice, API clients should use at least one subspace for
// application data. For general guidance on subspace usage, see the Subspaces
// section of the Developer Guide
// (https://foundationdb.com/documentation/developer-guide.html#developer-guide-sub-keyspaces).
package subspace

import (
	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/FoundationDB/fdb-go/fdb/tuple"
	"bytes"
	"errors"
)

// Subspace represents a well-defined region of keyspace in a FoundationDB
// database.
type Subspace interface {
	// Sub returns a new Subspace whose prefix extends this Subspace with the
	// encoding of the provided element(s). If any of the elements are not a
	// valid tuple.TupleElement, Sub will panic.
	Sub(el ...tuple.TupleElement) Subspace

	// Bytes returns the literal bytes of the prefix of this Subspace.
	Bytes() []byte

	// Pack returns the key encoding the specified Tuple with the prefix of this
	// Subspace prepended.
	Pack(t tuple.Tuple) fdb.Key

	// Unpack returns the Tuple encoded by the given key with the prefix of this
	// Subspace removed. Unpack will return an error if the key is not in this
	// Subspace or does not encode a well-formed Tuple.
	Unpack(k fdb.KeyConvertible) (tuple.Tuple, error)

	// Contains returns true if the provided key starts with the prefix of this
	// Subspace, indicating that the Subspace logically contains the key.
	Contains(k fdb.KeyConvertible) bool

	// All Subspaces implement fdb.KeyConvertible and may be used as
	// FoundationDB keys (corresponding to the prefix of this Subspace).
	fdb.KeyConvertible

	// All Subspaces implement fdb.ExactRange and fdb.Range, and describe all
	// keys logically in this Subspace.
	fdb.ExactRange
	fdb.Range
}

type subspace struct {
	b []byte
}

// AllKeys returns the Subspace corresponding to all keys in a FoundationDB
// database.
func AllKeys() Subspace {
	return subspace{}
}

// Sub returns a new Subspace whose prefix is the encoding of the provided
// element(s). If any of the elements are not a valid tuple.TupleElement, a
// runtime panic will occur.
func Sub(el ...tuple.TupleElement) Subspace {
	return subspace{tuple.Tuple(el).Pack()}
}

// FromBytes returns a new Subspace from the provided bytes.
func FromBytes(b []byte) Subspace {
	s := make([]byte, len(b))
	copy(s, b)
	return subspace{b}
}

func (s subspace) Sub(el ...tuple.TupleElement) Subspace {
	return subspace{concat(s.Bytes(), tuple.Tuple(el).Pack()...)}
}

func (s subspace) Bytes() []byte {
	return s.b
}

func (s subspace) Pack(t tuple.Tuple) fdb.Key {
	return fdb.Key(concat(s.b, t.Pack()...))
}

func (s subspace) Unpack(k fdb.KeyConvertible) (tuple.Tuple, error) {
	key := k.FDBKey()
	if !bytes.HasPrefix(key, s.b) {
		return nil, errors.New("key is not in subspace")
	}
	return tuple.Unpack(key[len(s.b):])
}

func (s subspace) Contains(k fdb.KeyConvertible) bool {
	return bytes.HasPrefix(k.FDBKey(), s.b)
}

func (s subspace) FDBKey() fdb.Key {
	return fdb.Key(s.b)
}

func (s subspace) FDBRangeKeys() (fdb.KeyConvertible, fdb.KeyConvertible) {
	return fdb.Key(concat(s.b, 0x00)), fdb.Key(concat(s.b, 0xFF))
}

func (s subspace) FDBRangeKeySelectors() (fdb.Selectable, fdb.Selectable) {
	begin, end := s.FDBRangeKeys()
	return fdb.FirstGreaterOrEqual(begin), fdb.FirstGreaterOrEqual(end)
}

func concat(a []byte, b ...byte) []byte {
	r := make([]byte, len(a) + len(b))
	copy(r, a)
	copy(r[len(a):], b)
	return r
}
