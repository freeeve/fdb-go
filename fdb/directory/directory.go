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

// Package directory provides a tool for managing related subspaces. Directories
// are a recommended approach for administering applications. Each application
// should create or open at least one directory to manage its subspaces.
//
// For general guidance on directory usage, see the Directories section of the
// Developer Guide
// (http://foundationdb.com/documentation/developer-guide.html#developer-guide-directories).
//
// Directories are identified by hierarchical paths analogous to the paths in a
// Unix-like file system. A path is represented as a slice of strings. Each
// directory has an associated subspace used to store its content. The directory
// layer maps each path to a short prefix used for the corresponding
// subspace. In effect, directories provide a level of indirection for access to
// subspaces.
//
// Directory operations are transactional. A byte slice layer option is used as
// a metadata identifier when opening a directory.
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

// Directory represents a subspace of keys in a FoundationDB database,
// identified by a heirarchical path.
//
// Unless otherwise noted, methods of Directory interpret the provided path(s)
// relative to the path of the Directory.
type Directory interface {
	// CreateOrOpen opens the directory specified by path, and returns the
	// directory and its contents as a DirectorySubspace. If the directory does
	// not exist, it is created (creating parent directories if necessary).
	//
	// If the byte slice layer is specified and the directory is new, it is
	// recorded as the layer; if layer is specified and the directory already
	// exists, it is compared against the layer specified when the directory was
	// created, and an error is returned if they differ.
	CreateOrOpen(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error)

	// Open opens the directory specified by path, and returns the directory and
	// its contents as a DirectorySubspace (or an error if the directory does
	// not exist).
	//
	// If the byte slice layer is specified, it is compared against the layer
	// specified when the directory was created, and an error is returned if
	// they differ.
	Open(rt fdb.ReadTransactor, path []string, layer []byte) (DirectorySubspace, error)

	// Create creates a directory specified by path, and returns the directory
	// and its contents as a DirectorySubspace (or an error if the directory
	// already exists).
	//
	// If the byte slice layer is specified, it is recorded as the layer and
	// will be checked when opening the directory in the future.
	Create(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error)

	// CreatePrefix behaves like Create, but uses a manually specified byte
	// slice prefix to physically store the contents of this directory, rather
	// than an automatically allocated prefix.
	CreatePrefix(t fdb.Transactor, path []string, layer []byte, prefix []byte) (DirectorySubspace, error)

	// Move moves the directory at oldPath to newPath, and returns the directory
	// (at its new location) and its contents as a DirectorySubspace. Move will
	// return an error if a directory does not exist at oldPath, a directory
	// already exists at newPath, or the parent directory of newPath does not
	// exist.
	//
	// There is no effect on the physical prefix of the given directory or on
	// clients that already have the directory open.
	Move(t fdb.Transactor, oldPath []string, newPath []string) (DirectorySubspace, error)

	// MoveTo moves this directory to newAbsolutePath, and returns the directory
	// (at its new location) and its contents as a DirectorySubspace. MoveTo
	// will return an error if a directory already exists at newAbsolutePath or
	// the parent directory of newAbsolutePath does not exist.
	MoveTo(t fdb.Transactor, newAbsolutePath []string) (DirectorySubspace, error)

	// Remove removes the directory at path, its content, and all
	// subdirectories. Remove returns true if a directory existed at path and
	// was removed, and false if no directory exists at path.
	Remove(t fdb.Transactor, path []string) (bool, error)

	// Exists returns true if the directory at path exists and false otherwise.
	Exists(rt fdb.ReadTransactor, path []string) (bool, error)

	// List returns the names of the immediate subdirectories of the directory
	// at path as a slice of strings. Each string is the name of the last
	// component of a subdirectory's path.
	List(rt fdb.ReadTransactor, path []string) ([]string, error)

	// GetLayer returns the layer specified when the directory was created.
	GetLayer() []byte

	// GetPath returns the path of the directory. FIXME: this path can't be
	// trusted if this directory has been moved since opening?
	GetPath() []string
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

var root = NewDirectoryLayer(subspace.FromBytes([]byte{0xFE}), subspace.AllKeys(), false)

// CreateOrOpen opens the directory specified by path, and returns the directory
// and its contents as a DirectorySubspace. If the directory does not exist, it
// is created (creating parent directories if necessary).
//
// If the byte slice layer is specified and the directory is new, it is recorded
// as the layer; if layer is specified and the directory already exists, it is
// compared against the layer specified when the directory was created, and an
// error is returned if they differ.
func CreateOrOpen(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	return root.CreateOrOpen(t, path, layer)
}

// Open opens the directory specified by path, and returns the directory and its
// contents as a DirectorySubspace (or an error if the directory does not
// exist).
//
// If the byte slice layer is specified, it is compared against the layer
// specified when the directory was created, and an error is returned if they
// differ.
func Open(rt fdb.ReadTransactor, path []string, layer []byte) (DirectorySubspace, error) {
	return root.Open(rt, path, layer)
}

// Create creates a directory specified by path, and returns the directory and
// its contents as a DirectorySubspace (or an error if the directory already
// exists).
//
// If the byte slice layer is specified, it is recorded as the layer and will be
// checked when opening the directory in the future.
func Create(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	return root.Create(t, path, layer)
}

// CreatePrefix behaves like Create, but uses a manually specified byte slice
// prefix to physically store the contents of this directory, rather than an
// automatically allocated prefix.
func CreatePrefix(t fdb.Transactor, path []string, layer []byte, prefix []byte) (DirectorySubspace, error) {
	return root.CreatePrefix(t, path, layer, prefix)
}

// Move moves the directory at oldPath to newPath, and returns the directory (at
// its new location) and its contents as a DirectorySubspace. Move will return
// an error if a directory does not exist at oldPath, a directory already exists
// at newPath, or the parent directory of newPath does not exit.
//
// There is no effect on the physical prefix of the given directory or on
// clients that already have the directory open.
func Move(t fdb.Transactor, oldPath []string, newPath []string) (DirectorySubspace, error) {
	return root.Move(t, oldPath, newPath)
}

// Exists returns true if the directory at path exists and false otherwise.
func Exists(rt fdb.ReadTransactor, path []string) (bool, error) {
	return root.Exists(rt, path)
}

// List returns the names of the immediate subdirectories of the default root
// directory as a slice of strings. Each string is the name of the last
// component of a subdirectory's path.
func List(rt fdb.ReadTransactor, path []string) ([]string, error) {
	return root.List(rt, path)
}

// Root returns the default DirectoryLayer object as a Directory. Any attempt to
// move or remove the root directory will return an error.
//
// As an alternative to Root, you may use the package-level functions
// CreateOrOpen, Open, Create, CreatePrefix, Move, Exists and List to operate
// directly on the default DirectoryLayer.
func Root() Directory {
	return root
}
