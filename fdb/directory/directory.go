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
)

const SUBDIRS int = 0

// []int32{1,0,0} by any other name
const MAJORVERSION int32 = 1
const MINORVERSION int32 = 0
const MICROVERSION int32 = 0

type Error struct {
	message string
}

func (e Error) Error() string {
	return e.message
}

type Directory interface {
	CreateOrOpen(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error)

	Create(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error)
	CreatePrefix(t fdb.Transactor, path []string, layer []byte, prefix []byte) (DirectorySubspace, error)

	Open(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error)

	MoveTo(t fdb.Transactor, newAbsolutePath []string) (DirectorySubspace, error)
	Move(t fdb.Transactor, oldPath []string, newPath []string) (DirectorySubspace, error)

	Remove(t fdb.Transactor, path []string) (bool, error)

	Exists(t fdb.Transactor, path []string) (bool, error)

	List(t fdb.Transactor, path []string) ([]string, error)

	CheckLayer(layer []byte) error

	getLayerForPath(path []string) DirectoryLayer
}

var root *DirectoryLayer

func Root() Directory {
	if root == nil {
		dl := NewDirectoryLayer(subspace.FromRawBytes([]byte{0xFE}), subspace.AllKeys(), []string{})
		root = &dl
	}

	return *root
}
