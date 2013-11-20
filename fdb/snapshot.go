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

// Snapshot is a handle to a FoundationDB transaction snapshot, suitable for
// performing snapshot reads. Snapshot reads offer a more relaxed isolation
// level than FoundationDB's default serializable isolation, reducing
// transaction conflicts but making it harder to reason about concurrency.
//
// For more information on snapshot reads, see
// https://foundationdb.com/documentation/developer-guide.html#snapshot-reads.
type Snapshot struct {
	*transaction
}

// ReadTransact passes the Snapshot receiver object to the caller-provided
// function (as a ReadTransaction), but does not handle errors or commit the
// transaction.
//
// ReadTransact makes Snapshot satisfy the ReadTransactor interface, allowing
// read-only transactional functions to be used compositionally.
//
// See the ReadTransactor interface for an example of using ReadTransact with
// Transaction, Snapshot and Database objects.
func (s Snapshot) ReadTransact(f func (ReadTransaction) (interface{}, error)) (interface{}, error) {
	return f(s)
}

// Snapshot returns the receiver and allows Snapshot to satisfy the
// ReadTransaction interface.
func (s Snapshot) Snapshot() Snapshot {
	return s
}

// Get is equivalent to (Transaction).Get(), performed as a snapshot read.
func (s Snapshot) Get(key KeyConvertible) FutureValue {
	return s.get(key.ToFDBKey(), 1)
}

// GetKey is equivalent to (Transaction).GetKey(), performed but as a snapshot
// read.
func (s Snapshot) GetKey(sel Selectable) FutureKey {
	return s.getKey(sel.ToFDBKeySelector(), 1)
}

// GetRange is equivalent to (Transaction).GetRange(), performed but as a
// snapshot read.
func (s Snapshot) GetRange(r Range, options RangeOptions) RangeResult {
	return s.getRange(r, options, true)
}

// GetReadVersion is equivalent to (Transaction).GetReadVersion(), performed as
// a snapshot read.
func (s Snapshot) GetReadVersion() FutureVersion {
	return s.getReadVersion()
}

// GetDatabase returns a handle to the database with which this snapshot is
// interacting.
func (s Snapshot) GetDatabase() Database {
	return s.transaction.db
}
