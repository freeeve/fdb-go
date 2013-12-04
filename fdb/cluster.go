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

// Cluster is a handle to a FoundationDB cluster. Cluster is a lightweight
// object that may be efficiently copied, and is safe for concurrent use by
// multiple goroutines.
//
// It is generally preferable to use Open or OpenDefault to obtain a database
// handle directly.
type Cluster struct {
	*cluster
}

type cluster struct {
	ptr *C.FDBCluster
}

func (c *cluster) destroy() {
	C.fdb_cluster_destroy(c.ptr)
}

// OpenDatabase returns a database handle from the FoundationDB cluster. It is
// generally preferable to use Open or OpenDefault to obtain a database handle
// directly.
//
// In the current release, the database name must be []byte("DB").
func (c Cluster) OpenDatabase(dbName []byte) (Database, error) {
	f := C.fdb_cluster_create_database(c.ptr, byteSliceToPtr(dbName), C.int(len(dbName)))
	fdb_future_block_until_ready(f)

	var outd *C.FDBDatabase

	if err := C.fdb_future_get_database(f, &outd); err != 0 {
		return Database{}, Error{int(err)}
	}

	C.fdb_future_destroy(f)

	d := &database{outd}
	runtime.SetFinalizer(d, (*database).destroy)

	return Database{d}, nil
}
