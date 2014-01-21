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

package directory

import (
	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/FoundationDB/fdb-go/fdb/subspace"
)

// DirectorySubspace represents a Directory that may also be used as a Subspace
// to store key/value pairs. Subdirectories of a root directory (as returned by
// Root or NewDirectoryLayer) are DirectorySubspaces, and provide all methods of
// the Directory and subspace.Subspace interfaces.
type DirectorySubspace interface {
	subspace.Subspace
	Directory
}

type directorySubspace struct {
	subspace.Subspace
	dl directoryLayer
	path []string
	layer []byte
}

func (d directorySubspace) CreateOrOpen(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	return d.dl.CreateOrOpen(t, d.dl.partitionSubpath(d.path, path), layer)
}

func (d directorySubspace) Create(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	return d.dl.Create(t, d.dl.partitionSubpath(d.path, path), layer)
}

func (d directorySubspace) CreatePrefix(t fdb.Transactor, path []string, layer []byte, prefix []byte) (DirectorySubspace, error) {
	return d.dl.CreatePrefix(t, d.dl.partitionSubpath(d.path, path), layer, prefix)
}

func (d directorySubspace) Open(rt fdb.ReadTransactor, path []string, layer []byte) (DirectorySubspace, error) {
	return d.dl.Open(rt, d.dl.partitionSubpath(d.path, path), layer)
}

func (d directorySubspace) MoveTo(t fdb.Transactor, newAbsolutePath []string) (DirectorySubspace, error) {
	return moveTo(t, d.dl, d.path, newAbsolutePath)
}

func (d directorySubspace) Move(t fdb.Transactor, oldPath []string, newPath []string) (DirectorySubspace, error) {
	return d.dl.Move(t, d.dl.partitionSubpath(d.path, oldPath), d.dl.partitionSubpath(d.path, newPath))
}

func (d directorySubspace) Remove(t fdb.Transactor, path []string) (bool, error) {
	return d.dl.Remove(t, d.dl.partitionSubpath(d.path, path))
}

func (d directorySubspace) Exists(rt fdb.ReadTransactor, path []string) (bool, error) {
	return d.dl.Exists(rt, d.dl.partitionSubpath(d.path, path))
}

func (d directorySubspace) List(rt fdb.ReadTransactor, path []string) (subdirs []string, e error) {
	return d.dl.List(rt, d.dl.partitionSubpath(d.path, path))
}

func (d directorySubspace) GetLayer() []byte {
	return d.layer
}

func (d directorySubspace) GetPath() []string {
	return d.path
}
