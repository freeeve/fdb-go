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

// A Selectable can be converted to a FoundationDB KeySelector. All functions in
// the FoundationDB API that resolve a key selector to a key accept Selectable.
type Selectable interface {
	FDBKeySelector() KeySelector
}

// KeySelector represents a description of a key in a FoundationDB database. A
// KeySelector may be resolved to a specific key with the GetKey method, or used
// as the endpoints of a SelectorRange to be used with a GetRange function.
//
// The most common key selectors are constructed with the functions documented
// below. For details of how KeySelectors are specified and resolved, see
// https://foundationdb.com/documentation/developer-guide.html#key-selectors.
type KeySelector struct {
	Key KeyConvertible
	OrEqual bool
	Offset int
}

func (ks KeySelector) FDBKeySelector() KeySelector {
	return ks
}

// LastLessThan returns the KeySelector specifying the lexigraphically greatest
// key present in the database which is lexigraphically strictly less than the
// given key.
func LastLessThan(key KeyConvertible) KeySelector {
	return KeySelector{key, false, 0}
}

// LastLessOrEqual returns the KeySelector specifying the lexigraphically
// greatest key present in the database which is lexigraphically less than or
// equal to the given key.
func LastLessOrEqual(key KeyConvertible) KeySelector {
	return KeySelector{key, true, 0}
}

// FirstGreaterThan returns the KeySelector specifying the lexigraphically least
// key present in the database which is lexigraphically strictly greater than
// the given key.
func FirstGreaterThan(key KeyConvertible) KeySelector {
	return KeySelector{key, true, 1}
}

// FirstGreaterOrEqual returns the KeySelector specifying the lexigraphically
// least key present in the database which is lexigraphically greater than or
// equal to the given key.
func FirstGreaterOrEqual(key KeyConvertible) KeySelector {
	return KeySelector{key, false, 1}
}
