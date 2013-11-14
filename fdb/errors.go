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
 #define FDB_API_VERSION 100
 #include <foundationdb/fdb_c.h>
*/
import "C"

import (
	"fmt"
)

// FDBError represents a low-level error returned by the FoundationDB C
// library. An FDBError may be returned by any FoundationDB API function that
// returns error, or as a panic from any FoundationDB API function whose name
// ends with OrPanic.
//
// You may compare the Code field of an FDBError against the list of
// FoundationDB error codes at
// https://foundationdb.com/documentation/api-error-codes.html, but generally an
// FDBError should be passed to (Transaction).OnError. When using
// (Database).Transact, non-fatal errors will be retried automatically.
type FDBError struct {
	Code int
}

func (e FDBError) Error() string {
	return fmt.Sprintf("FoundationDB error code %d (%s)", e.Code, C.GoString(C.fdb_get_error(C.fdb_error_t(e.Code))))
}

// SOMEDAY: these (along with others) should be coming from fdb.options?

var errNetworkNotSetup = FDBError{2008}

var errAPIVersionUnset = FDBError{2200}
var errAPIVersionAlreadySet = FDBError{2201}
var errAPIVersionNotSupported = FDBError{2203}
