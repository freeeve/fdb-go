// FoundationDB Go Directory Layer
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
package directory

import (
	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/FoundationDB/fdb-go/fdb/subspace"
	"errors"
)

const (
	_SUBDIRS int = 0

	// []int32{1,0,0} by any other name
	_MAJORVERSION int32 = 1
	_MINORVERSION int32 = 0
	_MICROVERSION int32 = 0
)

type Directory interface {
	CreateOrOpen(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error)

	Create(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error)
	CreatePrefix(t fdb.Transactor, path []string, layer []byte, prefix []byte) (DirectorySubspace, error)

	Open(rt fdb.ReadTransactor, path []string, layer []byte) (DirectorySubspace, error)

	MoveTo(t fdb.Transactor, newAbsolutePath []string) (DirectorySubspace, error)
	Move(t fdb.Transactor, oldPath []string, newPath []string) (DirectorySubspace, error)

	Remove(t fdb.Transactor, path []string) (bool, error)

	Exists(rt fdb.ReadTransactor, path []string) (bool, error)

	List(rt fdb.ReadTransactor, path []string) ([]string, error)

	GetLayer() []byte
	GetPath() []string

	getLayerForPath(path []string) DirectoryLayer
}

func stringsEqual(a, b []string) bool {
    if len(a) != len(b) {
        return false
    }
    for i, v := range a {
        if v != b[i] {
            return false
        }
    }
    return true
}

func moveTo(t fdb.Transactor, dl DirectoryLayer, path, newAbsolutePath []string) (DirectorySubspace, error) {
	partition_len := len(dl.path)

	if !stringsEqual(newAbsolutePath[:partition_len], dl.path) {
		return nil, errors.New("cannot move between partitions")
	}

	return dl.Move(t, path[partition_len:], newAbsolutePath[partition_len:])
}

var root = NewDirectoryLayer(subspace.FromBytes([]byte{0xFE}), subspace.AllKeys())

func Root() Directory {
	return root
}
