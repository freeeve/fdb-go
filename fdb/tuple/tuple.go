// FoundationDB Go Tuple Layer
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

// Package tuple provides a layer for encoding and decoding multi-element tuples
// into keys usable by FoundationDB. The encoded key maintains the same sort
// order as the original tuple: sorted first by the first element, then by the
// second element, etc. This makes the tuple layer ideal for building a variety
// of higher-level data models.
//
// FoundationDB tuples can currently encode byte and unicode strings, integers
// and NULL values. In Go these are represented as []byte, string, int64 and
// nil.
package tuple

import (
	"fmt"
	"encoding/binary"
	"bytes"
	"github.com/FoundationDB/fdb-go/fdb"
)

// TupleElement describes types that may be encoded in FoundationDB
// tuples. Although the Go compiler cannot enforce this, it is a programming
// error to use objects of unsupported types as TupleElements (and will
// typically result in a runtime panic).
//
// The valid types for TupleElement are []byte (or fdb.KeyConvertible), string,
// int64 (or int), and nil.
type TupleElement interface{}

// Tuple is a slice of objects that can be encoded as FoundationDB tuples. If
// any of the TupleElements are of unsupported types, a runtime panic will occur
// when the Tuple is packed.
//
// Given a Tuple T containing objects only of these types, then T will be
// identical to the Tuple returned by unpacking the byte slice obtained by
// packing T (modulo type normalization to []byte and int64).
type Tuple []TupleElement

var sizeLimits = []uint64{
	1 << (0 * 8) - 1,
	1 << (1 * 8) - 1,
	1 << (2 * 8) - 1,
	1 << (3 * 8) - 1,
	1 << (4 * 8) - 1,
	1 << (5 * 8) - 1,
	1 << (6 * 8) - 1,
	1 << (7 * 8) - 1,
	1 << (8 * 8) - 1,
}

func encodeBytes(buf *bytes.Buffer, code byte, b []byte) {
	buf.WriteByte(code)
	buf.Write(bytes.Replace(b, []byte{0x00}, []byte{0x00, 0xFF}, -1))
	buf.WriteByte(0x00)
}

func bisectLeft(u uint64) int {
	var n int
	for sizeLimits[n] < u {
		n += 1
	}
	return n
}

func encodeInt(buf *bytes.Buffer, i int64) {
	if i == 0 {
		buf.WriteByte(0x14)
		return
	}

	var n int
	var ibuf bytes.Buffer

	switch {
	case i > 0:
		n = bisectLeft(uint64(i))
		buf.WriteByte(byte(0x14+n))
		binary.Write(&ibuf, binary.BigEndian, i)
	case i < 0:
		n = bisectLeft(uint64(-i))
		buf.WriteByte(byte(0x14-n))
		binary.Write(&ibuf, binary.BigEndian, int64(sizeLimits[n])+i)
	}

	buf.Write(ibuf.Bytes()[8-n:])
}

// Pack returns a new byte slice encoding the provided tuple. Pack will panic if
// the tuple contains an element of any type other than []byte,
// fdb.KeyConvertible, string, int64, int or nil.
func (t Tuple) Pack() fdb.Key {
	buf := new(bytes.Buffer)

	for i, e := range(t) {
		switch e := e.(type) {
		case nil:
			buf.WriteByte(0x00)
		case int64:
			encodeInt(buf, e)
		case int:
			encodeInt(buf, int64(e))
		case []byte:
			encodeBytes(buf, 0x01, e)
		case fdb.KeyConvertible:
			encodeBytes(buf, 0x01, []byte(e.FDBKey()))
		case string:
			encodeBytes(buf, 0x02, []byte(e))
		default:
			panic(fmt.Sprintf("unencodable element at index %d (%v, type %T)", i, t[i], t[i]))
		}
	}

	return fdb.Key(buf.Bytes())
}

func findTerminator(b []byte) int {
	bp := b
	var length int

	for {
		idx := bytes.IndexByte(bp, 0x00)
		length += idx
		if idx + 1 == len(bp) || bp[idx+1] != 0xFF {
			break
		}
		length += 2
		bp = bp[idx+2:]
	}

	return length
}

func decodeBytes(b []byte) ([]byte, int) {
	idx := findTerminator(b[1:])
	return bytes.Replace(b[1:idx+1], []byte{0x00, 0xFF}, []byte{0x00}, -1), idx + 2
}

func decodeString(b []byte) (string, int) {
	bp, idx := decodeBytes(b)
	return string(bp), idx
}

func decodeInt(b []byte) (int64, int) {
	if b[0] == 0x14 {
		return 0, 1
	}

	var neg bool

	n := int(b[0]) - 20
	if n < 0 {
		n = -n
		neg = true
	}

	bp := make([]byte, 8)
	copy(bp[8-n:], b[1:n+1])

	var ret int64

	binary.Read(bytes.NewBuffer(bp), binary.BigEndian, &ret)

	if neg {
		ret -= int64(sizeLimits[n])
	}

	return ret, n+1
}

// Unpack returns the tuple encoded by the provided byte slice, or an error if
// the key does not correctly encode a FoundationDB tuple.
func Unpack(k fdb.KeyConvertible) (Tuple, error) {
	var t Tuple

	b := k.FDBKey()
	var i int

	for i < len(b) {
		var el interface{}
		var off int

		switch {
		case b[i] == 0x00:
			el = nil
			off = 1
		case b[i] == 0x01:
			el, off = decodeBytes(b[i:])
		case b[i] == 0x02:
			el, off = decodeString(b[i:])
		case 0x0c <= b[i] && b[i] <= 0x1c:
			el, off = decodeInt(b[i:])
		default:
			return nil, fmt.Errorf("unable to decode tuple element with unknown typecode %02x", b[i])
		}

		t = append(t, el)
		i += off
	}

	return t, nil
}

// FIXME: document
func (t Tuple) FDBKey() fdb.Key {
	return t.Pack()
}

// FIXME: document
func (t Tuple) FDBRangeKeys() (fdb.KeyConvertible, fdb.KeyConvertible) {
	p := t.Pack()
	return fdb.Key(concat(p, 0x00)), fdb.Key(concat(p, 0xFF))
}

// FIXME: document
func (t Tuple) FDBRangeKeySelectors() (fdb.Selectable, fdb.Selectable) {
	b, e := t.FDBRangeKeys()
	return fdb.FirstGreaterOrEqual(b), fdb.FirstGreaterOrEqual(e)
}

func concat(a []byte, b ...byte) []byte {
	r := make([]byte, len(a) + len(b))
	copy(r, a)
	copy(r[len(a):], b)
	return r
}
