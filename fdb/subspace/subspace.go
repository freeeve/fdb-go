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

// FIXME: document
package subspace

import (
	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/FoundationDB/fdb-go/fdb/tuple"
	"bytes"
	"fmt"
)

type Subspace interface {
	Sub(el ...interface{}) Subspace

	Key() fdb.Key

	Pack(t tuple.Tuple) []byte
	Unpack(k fdb.KeyConvertible) (tuple.Tuple, error)

	Contains(k fdb.KeyConvertible) bool

	fdb.KeyConvertible
	fdb.ExactRange
}

type subspace []byte

func AllKeys() Subspace {
	return subspace{}
}

func FromTuple(t tuple.Tuple) Subspace {
	return AllKeys().Sub(t...)
}

func FromRawBytes(raw []byte) Subspace {
	ss := make([]byte, len(raw))
	copy(ss, raw)
	return subspace(ss)
}

func (s subspace) Sub(el ...interface{}) Subspace {
	return append(append(subspace{}, s...), tuple.Tuple(el).Pack()...)
}

func (s subspace) Key() fdb.Key {
	return fdb.Key(s)
}

func (s subspace) Pack(tuple tuple.Tuple) []byte {
	return append(append(subspace{}, s...), tuple.Pack()...)
}

func (s subspace) Unpack(k fdb.KeyConvertible) (tuple.Tuple, error) {
	key := k.ToFDBKey()
	if !bytes.HasPrefix(key, s) {
		return nil, fmt.Errorf("Key is not in subspace")
	}
	return tuple.Unpack(key[len(s):])
}

func (s subspace) Contains(k fdb.KeyConvertible) bool {
	return bytes.HasPrefix(k.ToFDBKey(), s)
}

func (s subspace) ToFDBKey() fdb.Key {
	return fdb.Key(s)
}

func (s subspace) BeginKey() fdb.Key {
	kr := tuple.Tuple{}.Range()
	return append(append(fdb.Key{}, s...), kr.BeginKey()...)
}

func (s subspace) EndKey() fdb.Key {
	kr := tuple.Tuple{}.Range()
	return append(append(fdb.Key{}, s...), kr.EndKey()...)
}

func (s subspace) BeginKeySelector() fdb.KeySelector {
	return fdb.FirstGreaterOrEqual(s.BeginKey())
}

func (s subspace) EndKeySelector() fdb.KeySelector {
	return fdb.FirstGreaterOrEqual(s.EndKey())
}
