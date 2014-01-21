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
	"github.com/FoundationDB/fdb-go/fdb/tuple"
)

type directoryPartition struct {
	directoryLayer
	parentDirectoryLayer directoryLayer
}

func (dp directoryPartition) Sub(el ...tuple.TupleElement) subspace.Subspace {
	panic("cannot open subspace in the root of a directory partition")
}

func (dp directoryPartition) Bytes() []byte {
	panic("cannot get key for the root of a directory partition")
}

func (dp directoryPartition) Pack(t tuple.Tuple) fdb.Key {
	panic("cannot pack keys using the root of a directory partition")
}

func (dp directoryPartition) Unpack(k fdb.KeyConvertible) (tuple.Tuple, error) {
	panic("cannot unpack keys using the root of a directory partition")
}

func (dp directoryPartition) Contains(k fdb.KeyConvertible) bool {
	panic("cannot check whether a key belongs to the root of a directory partition")
}

func (dp directoryPartition) FDBKey() fdb.Key {
	panic("cannot use the root of a directory partition as a key")
}

func (dp directoryPartition) FDBRangeKeys() (fdb.KeyConvertible, fdb.KeyConvertible) {
	panic("cannot get range for the root of a directory partition")
}

func (dp directoryPartition) FDBRangeKeySelectors() (fdb.Selectable, fdb.Selectable) {
	panic("cannot get range for the root of a directory partition")
}

func (dp directoryPartition) GetLayer() []byte {
	return []byte("partition")
}

func (dp directoryPartition) getLayerForPath(path []string) directoryLayer {
	if len(path) == 0 {
		return dp.parentDirectoryLayer
	} else {
		return dp.directoryLayer
	}
}

func (dp directoryPartition) MoveTo(t fdb.Transactor, newAbsolutePath []string) (DirectorySubspace, error) {
	return moveTo(t, dp.parentDirectoryLayer, dp.path, newAbsolutePath)
}

func (dp directoryPartition) Remove(t fdb.Transactor, path []string) (bool, error) {
	dl := dp.getLayerForPath(path)
	return dl.Remove(t, dl.partitionSubpath(dp.path, path))
}

func (dp directoryPartition) Exists(rt fdb.ReadTransactor, path []string) (bool, error) {
	dl := dp.getLayerForPath(path)
	return dl.Exists(rt, dl.partitionSubpath(dp.path, path))
}
