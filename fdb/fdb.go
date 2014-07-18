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
 #include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"sync"
	"unsafe"
)

/* Would put this in futures.go but for the documented issue with
/* exports and functions in preamble
/* (https://code.google.com/p/go-wiki/wiki/cgo#Global_functions) */
//export notifyChannel
func notifyChannel(ch *chan struct{}) {
	close(*ch)
}

// A Transactor can execute a function that requires a Transaction. Functions
// written to accept a Transactor are called transactional functions, and may be
// called with either a Database or a Transaction.
type Transactor interface {
	// Transact executes the caller-provided function, providing it with a
	// Transaction (itself a Transactor, allowing composition of transactional
	// functions).
	Transact(func(Transaction) (interface{}, error)) (interface{}, error)

	// All Transactors are also ReadTransactors, allowing them to be used with
	// read-only transactional functions.
	ReadTransactor
}

// A ReadTransactor can execute a function that requires a
// ReadTransaction. Functions written to accept a ReadTransactor are called
// read-only transactional functions, and may be called with a Database,
// Transaction or Snapshot.
type ReadTransactor interface {
	// ReadTransact executes the caller-provided function, providing it with a
	// ReadTransaction (itself a ReadTransactor, allowing composition of
	// read-only transactional functions).
	ReadTransact(func(ReadTransaction) (interface{}, error)) (interface{}, error)
}

func setOpt(setter func(*C.uint8_t, C.int) C.fdb_error_t, param []byte) error {
	if err := setter(byteSliceToPtr(param), C.int(len(param))); err != 0 {
		return Error{int(err)}
	}

	return nil
}

// NetworkOptions is a handle with which to set options that affect the entire
// FoundationDB client. A NetworkOptions instance should be obtained with the
// fdb.Options function.
type NetworkOptions struct {
}

// Options returns a NetworkOptions instance suitable for setting options that
// affect the entire FoundationDB client.
func Options() NetworkOptions {
	return NetworkOptions{}
}

func (opt NetworkOptions) setOpt(code int, param []byte) error {
	networkMutex.Lock()
	defer networkMutex.Unlock()

	if apiVersion == 0 {
		return errAPIVersionUnset
	}

	return setOpt(func(p *C.uint8_t, pl C.int) C.fdb_error_t {
		return C.fdb_network_set_option(C.FDBNetworkOption(code), p, pl)
	}, param)
}

// APIVersion determines the runtime behavior the fdb package. If the requested
// version is not supported by both the fdb package and the FoundationDB C
// library, an error will be returned. APIVersion must be called prior to any
// other functions in the fdb package.
//
// Currently, only API version 200 is supported.
func APIVersion(version int) error {
	networkMutex.Lock()
	defer networkMutex.Unlock()

	if apiVersion != 0 {
		if apiVersion == version {
			return nil
		}
		return errAPIVersionAlreadySet
	}

	if version < 200 || version > 200 {
		return errAPIVersionNotSupported
	}

	if e := C.fdb_select_api_version_impl(C.int(version), 200); e != 0 {
		if e == 2203 {
			return fmt.Errorf("API version %d not supported by the installed FoundationDB C library", version)
		}
		return Error{int(e)}
	}

	apiVersion = version

	return nil
}

// MustAPIVersion is like APIVersion but panics if the API version is not
// supported.
func MustAPIVersion(version int) {
	err := APIVersion(version)
	if err != nil {
		panic(err)
	}
}

var apiVersion int
var networkStarted bool
var networkMutex sync.Mutex

var openClusters map[string]Cluster
var openDatabases map[string]Database

func init() {
	openClusters = make(map[string]Cluster)
	openDatabases = make(map[string]Database)
}

func startNetwork() error {
	if e := C.fdb_setup_network(); e != 0 {
		return Error{int(e)}
	}

	go C.fdb_run_network()

	networkStarted = true

	return nil
}

// StartNetwork initializes the FoundationDB client networking engine. It is not
// necessary to call StartNetwork when using the fdb.Open or fdb.OpenDefault
// functions to obtain a database handle. StartNetwork must not be called more
// than once.
func StartNetwork() error {
	networkMutex.Lock()
	defer networkMutex.Unlock()

	if apiVersion == 0 {
		return errAPIVersionUnset
	}

	return startNetwork()
}

// DefaultClusterFile should be passed to fdb.Open or fdb.CreateCluster to allow
// the FoundationDB C library to select the platform-appropriate default cluster
// file on the current machine.
const DefaultClusterFile string = ""

// OpenDefault returns a database handle to the default database from the
// FoundationDB cluster identified by the DefaultClusterFile on the current
// machine. The FoundationDB client networking engine will be initialized first,
// if necessary.
func OpenDefault() (Database, error) {
	return Open(DefaultClusterFile, []byte("DB"))
}

// MustOpenDefault is like OpenDefault but panics if the default database cannot
// be opened.
func MustOpenDefault() Database {
	db, err := OpenDefault()
	if err != nil {
		panic(err)
	}
	return db
}

// Open returns a database handle to the named database from the FoundationDB
// cluster identified by the provided cluster file and database name. The
// FoundationDB client networking engine will be initialized first, if
// necessary.
//
// In the current release, the database name must be []byte("DB").
func Open(clusterFile string, dbName []byte) (Database, error) {
	networkMutex.Lock()
	defer networkMutex.Unlock()

	if apiVersion == 0 {
		return Database{}, errAPIVersionUnset
	}

	var e error

	if !networkStarted {
		e = startNetwork()
		if e != nil {
			return Database{}, e
		}
	}

	cluster, ok := openClusters[clusterFile]
	if !ok {
		cluster, e = createCluster(clusterFile)
		if e != nil {
			return Database{}, e
		}
		openClusters[clusterFile] = cluster
	}

	db, ok := openDatabases[string(dbName)]
	if !ok {
		db, e = cluster.OpenDatabase(dbName)
		if e != nil {
			return Database{}, e
		}
		openDatabases[string(dbName)] = db
	}

	return db, nil
}

// MustOpen is like Open but panics if the database cannot be opened.
func MustOpen(clusterFile string, dbName []byte) Database {
	db, err := Open(clusterFile, dbName)
	if err != nil {
		panic(err)
	}
	return db
}

func createCluster(clusterFile string) (Cluster, error) {
	var cf *C.char

	if len(clusterFile) != 0 {
		cf = C.CString(clusterFile)
		defer C.free(unsafe.Pointer(cf))
	}

	f := C.fdb_create_cluster(cf)
	fdb_future_block_until_ready(f)

	var outc *C.FDBCluster

	if err := C.fdb_future_get_cluster(f, &outc); err != 0 {
		return Cluster{}, Error{int(err)}
	}

	C.fdb_future_destroy(f)

	c := &cluster{outc}
	runtime.SetFinalizer(c, (*cluster).destroy)

	return Cluster{c}, nil
}

// CreateCluster returns a cluster handle to the FoundationDB cluster identified
// by the provided cluster file.
func CreateCluster(clusterFile string) (Cluster, error) {
	networkMutex.Lock()
	defer networkMutex.Unlock()

	if apiVersion == 0 {
		return Cluster{}, errAPIVersionUnset
	}

	if !networkStarted {
		return Cluster{}, errNetworkNotSetup
	}

	return createCluster(clusterFile)
}

func byteSliceToPtr(b []byte) *C.uint8_t {
	if len(b) > 0 {
		return (*C.uint8_t)(unsafe.Pointer(&b[0]))
	} else {
		return nil
	}
}

// A KeyConvertible can be converted to a FoundationDB Key. All functions in the
// FoundationDB API that address a specific key accept a KeyConvertible.
type KeyConvertible interface {
	FDBKey() Key
}

// Key represents a FoundationDB key, a lexicographically-ordered sequence of
// bytes. Key implements the KeyConvertible interface.
type Key []byte

// FDBKey allows Key to (trivially) satisfy the KeyConvertible interface.
func (k Key) FDBKey() Key {
	return k
}

func panicToError(e *error) {
	if r := recover(); r != nil {
		fe, ok := r.(Error)
		if ok {
			*e = fe
		} else {
			panic(r)
		}
	}
}
