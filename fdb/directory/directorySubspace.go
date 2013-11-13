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
	"bytes"
)

type DirectorySubspace interface {
	subspace.Subspace
	Directory
}

type directorySubspace struct {
	subspace.Subspace
	dl DirectoryLayer
	path []string
	layer []byte
}

func (d directorySubspace) partitionSubpath(path []string, dl *DirectoryLayer) []string {
	if dl == nil {
		dl = &d.dl
	}
	r := make([]string, len(d.path) - len(dl.path) + len(path))
	copy(r, d.path[len(dl.path):])
	copy(r[len(d.path) - len(dl.path):], path)
	return r
}

func (d directorySubspace) CreateOrOpen(t fdb.Transactor, path []string, layer []byte) (ds DirectorySubspace, e error) {
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		return d.dl.CreateOrOpen(tr, d.partitionSubpath(path, nil), layer)
	})
	if e == nil {
		ds = r.(DirectorySubspace)
	}
	return
}

func (d directorySubspace) Create(t fdb.Transactor, path []string, layer []byte) (ds DirectorySubspace, e error) {
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		return d.dl.Create(tr, d.partitionSubpath(path, nil), layer)
	})
	if e == nil {
		ds = r.(DirectorySubspace)
	}
	return
}

func (d directorySubspace) CreatePrefix(t fdb.Transactor, path []string, layer []byte, prefix []byte) (ds DirectorySubspace, e error) {
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		return d.dl.CreatePrefix(tr, d.partitionSubpath(path, nil), layer, prefix)
	})
	if e == nil {
		ds = r.(DirectorySubspace)
	}
	return
}

func (d directorySubspace) Open(t fdb.Transactor, path []string, layer []byte) (ds DirectorySubspace, e error) {
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		return d.dl.Open(tr, d.partitionSubpath(path, nil), layer)
	})
	if e == nil {
		ds = r.(DirectorySubspace)
	}
	return
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

func (d directorySubspace) MoveTo(t fdb.Transactor, newAbsolutePath []string) (ds DirectorySubspace, e error) {
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		dl := d.getLayerForPath([]string{})
		partition_len := len(dl.path)
		partition_path := newAbsolutePath[:partition_len]

		if !stringsEqual(partition_path, dl.path) {
			return nil, Error{"Cannot move between partitions."}
		}

		return dl.Move(tr, d.path[partition_len:], newAbsolutePath[partition_len:])
	})
	if e == nil {
		ds = r.(DirectorySubspace)
	}
	return
}

func (d directorySubspace) Move(t fdb.Transactor, oldPath []string, newPath []string) (ds DirectorySubspace, e error) {
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		return d.dl.Move(tr, d.partitionSubpath(oldPath, nil), d.partitionSubpath(newPath, nil))
	})
	if e == nil {
		ds = r.(DirectorySubspace)
	}
	return
}

func (d directorySubspace) Remove(t fdb.Transactor, path []string) (bool, error) {
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		dl := d.getLayerForPath(path)
		return dl.Remove(tr, d.partitionSubpath(path, &dl))
	})
	return r.(bool), e
}

func (d directorySubspace) Exists(t fdb.Transactor, path []string) (bool, error) {
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		dl := d.getLayerForPath(path)
		return dl.Exists(tr, d.partitionSubpath(path, &dl))
	})
	return r.(bool), e
}

func (d directorySubspace) List(t fdb.Transactor, path []string) (subdirs []string, e error) {
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		return d.dl.List(tr, d.partitionSubpath(path, nil))
	})
	if e == nil {
		subdirs = r.([]string)
	}
	return
}

func (d directorySubspace) CheckLayer(layer []byte) error {
	if layer != nil && bytes.Compare(layer, d.layer) != 0 {
		return Error{"The directory was created with an incompatible layer."}
	}
	return nil
}

func (d directorySubspace) getLayerForPath(path []string) DirectoryLayer {
	return d.dl
}
