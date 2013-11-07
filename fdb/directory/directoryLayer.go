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
	"github.com/FoundationDB/fdb-go/fdb/tuple"
	"encoding/binary"
	"bytes"
	"fmt"
)

type DirectoryLayer struct {
	nodeSS subspace.Subspace
	contentSS subspace.Subspace

	allocator highContentionAllocator
	rootNode subspace.Subspace

	path []string
}

func NewDirectoryLayer(nodeSS subspace.Subspace, contentSS subspace.Subspace, path []string) DirectoryLayer {
	var dl DirectoryLayer

	dl.nodeSS = nodeSS
	dl.contentSS = contentSS

	dl.rootNode = dl.nodeSS.Sub(dl.nodeSS.Key())
	dl.allocator = newHCA(dl.rootNode.Sub([]byte("hca")))

	dl.path = path

	return dl
}

func (dl DirectoryLayer) DirectoryLayer() DirectoryLayer {
	return dl
}

func (dl DirectoryLayer) getLayerForPath(path []string) DirectoryLayer {
	return dl
}

func (dl DirectoryLayer) createOrOpen(tr fdb.Transaction, path []string, layer []byte, prefix []byte, allowCreate bool, allowOpen bool) DirectorySubspace {
	var e error

	e = dl.checkVersion(tr, false)
	if e != nil {
		panic(e)
	}

	if len(path) == 0 {
		// FIXME: specific error type?
		panic(Error{"The root directory cannot be opened."})
	}

	existingNode := dl.find(tr, path).prefetchMetadata(tr)
	if existingNode.exists() {
		if existingNode.isInPartition(nil, false) {
			subpath := existingNode.getPartitionSubpath()
			return existingNode.getContents(dl, nil).DirectoryLayer().createOrOpen(tr, subpath, layer, prefix, allowCreate, allowOpen)
		}

		if !allowOpen {
			panic(Error{"The directory already exists."})
		}

		if layer != nil && bytes.Compare(existingNode.layer(nil).GetOrPanic(), layer) != 0 {
			panic(Error{"The directory was created with an incompatible layer."})
		}

		return existingNode.getContents(dl, nil)
	}

	if !allowCreate {
		panic(Error{"The directory does not exist."})
	}

	dl.checkVersion(tr, true)

	if prefix == nil {
		a, e := dl.allocator.allocate(tr)
		if e != nil {
			// FIXME?
			panic(e)
		}
		prefix = append(append([]byte{}, dl.contentSS.Key()...), a...)
	}

	if !dl.isPrefixFree(tr, prefix) {
		panic(Error{"The given prefix is already in use."})
	}

	var parentNode subspace.Subspace

	if len(path) > 1 {
		parentNode = dl.nodeWithPrefix(dl.createOrOpen(tr, path[:len(path)-1], nil, nil, true, true).Key())
	} else {
		parentNode = dl.rootNode
	}

	if parentNode == nil {
		// FIXME?
		panic(Error{"The parent directory doesn't exist."})
	}

	node := dl.nodeWithPrefix(prefix)
	tr.Set(parentNode.Sub(SUBDIRS, path[len(path)-1]), prefix)

	if layer == nil {
		layer = []byte{}
	}

	tr.Set(node.Sub([]byte("layer")), layer)

	return dl.contentsOfNode(node, path, layer)
}

func (dl DirectoryLayer) CreateOrOpen(t fdb.Transactor, path []string, layer []byte) (ds DirectorySubspace, e error) {
	defer func() {
		if r := recover(); r != nil {
			r, ok := r.(Error)
			if ok {	e = r } else { panic(r) }
		}
	}()
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		return dl.createOrOpen(tr, path, layer, nil, true, true), nil
	})
	return r.(DirectorySubspace), e
}

func (dl DirectoryLayer) Create(t fdb.Transactor, path []string, layer []byte) (ds DirectorySubspace, e error) {
	defer func() {
		if r := recover(); r != nil {
			r, ok := r.(Error)
			if ok {	e = r } else { panic(r) }
		}
	}()
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		return dl.createOrOpen(tr, path, layer, nil, true, false), nil
	})
	return r.(DirectorySubspace), e
}

func (dl DirectoryLayer) CreatePrefix(t fdb.Transactor, path []string, layer []byte, prefix []byte) (ds DirectorySubspace, e error) {
	defer func() {
		if r := recover(); r != nil {
			r, ok := r.(Error)
			if ok {	e = r } else { panic(r) }
		}
	}()
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		return dl.createOrOpen(tr, path, layer, prefix, true, false), nil
	})
	return r.(DirectorySubspace), e
}

func (dl DirectoryLayer) Open(t fdb.Transactor, path []string, layer []byte) (ds DirectorySubspace, e error) {
	defer func() {
		if r := recover(); r != nil {
			r, ok := r.(Error)
			if ok {	e = r } else { panic(r) }
		}
	}()
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		return dl.createOrOpen(tr, path, layer, nil, false, true), nil
	})
	return r.(DirectorySubspace), e
}

func (dl DirectoryLayer) Exists(t fdb.Transactor, path []string) (b bool, e error) {
	defer func() {
		if r := recover(); r != nil {
			r, ok := r.(Error)
			if ok {	e = r } else { panic(r) }
		}
	}()
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		e := dl.checkVersion(tr, false)
		if e != nil { return nil, e }

		node := dl.find(tr, path).prefetchMetadata(tr)
		if !node.exists() {
			return false, nil
		}

		if node.isInPartition(nil, false) {
			return node.getContents(dl, nil).Exists(tr, node.getPartitionSubpath())
		}

		return true, nil
	})
	return r.(bool), e
}

func (dl DirectoryLayer) List(t fdb.Transactor, path []string) (subdirs []string, e error) {
	defer func() {
		if r := recover(); r != nil {
			r, ok := r.(Error)
			if ok {	e = r } else { panic(r) }
		}
	}()
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		e := dl.checkVersion(tr, false)
		if e != nil {
			return nil, e
		}

		node := dl.find(tr, path).prefetchMetadata(tr)
		if !node.exists() {
			return nil, Error{"The directory does not exist."}
		}

		if node.isInPartition(nil, true) {
			return node.getContents(dl, nil).List(tr, node.getPartitionSubpath())
		}

		return dl.subdirNames(tr, node.subspace), nil
	})
	return r.([]string), e
}

func (dl DirectoryLayer) MoveTo(t fdb.Transactor, newAbsolutePath []string) (ds DirectorySubspace, e error) {
	return nil, Error{"The root directory cannot be moved."}
}

func (dl DirectoryLayer) Move(t fdb.Transactor, oldPath []string, newPath []string) (ds DirectorySubspace, e error) {
	defer func() {
		if r := recover(); r != nil {
			r, ok := r.(Error)
			if ok {	e = r } else { panic(r) }
		}
	}()
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		e := dl.checkVersion(tr, false)
		if e != nil { return nil, e }

		fmt.Println(oldPath, newPath)
		sliceEnd := len(oldPath)
		if sliceEnd > len(newPath) {
			sliceEnd = len(newPath)
		}
		if stringsEqual(oldPath, newPath[:sliceEnd]) {
			return nil, Error{"The destination directory cannot be a subdirectory of the source directory."}
		}

		oldNode := dl.find(tr, oldPath).prefetchMetadata(tr)
		newNode := dl.find(tr, newPath).prefetchMetadata(tr)

		if !oldNode.exists() {
			return nil, Error{"The source directory does not exist."}
		}

		if oldNode.isInPartition(nil, false) || newNode.isInPartition(nil, false) {
			if !oldNode.isInPartition(nil, false) || !newNode.isInPartition(nil, false) || !stringsEqual(oldNode.path, newNode.path) {
				return nil, Error{"Cannot move between partitions."}
			}

			return newNode.getContents(dl, nil).Move(tr, oldNode.getPartitionSubpath(), newNode.getPartitionSubpath())
		}

		if newNode.exists() {
			return nil, Error{"The destination directory already exists. Remove it first."}
		}

		parentNode := dl.find(tr, newPath[:len(newPath)-1])
		if !parentNode.exists() {
			return nil, Error{"The parent of the destination directory does not exist. Create it first."}
		}

		p, e := dl.nodeSS.Unpack(oldNode.subspace.Key())
		if e != nil { panic(e) }
		tr.Set(parentNode.subspace.Sub(SUBDIRS, newPath[len(newPath)-1]), p[0].([]byte))

		dl.removeFromParent(tr, oldPath)

		return dl.contentsOfNode(oldNode.subspace, newPath, oldNode.layer(nil).GetOrPanic()), nil
	})
	if e == nil {
		ds = r.(DirectorySubspace)
	}
	return
}

func (dl DirectoryLayer) Remove(t fdb.Transactor, path []string) (removed bool, e error) {
	defer func() {
		if r := recover(); r != nil {
			r, ok := r.(Error)
			if ok {	e = r } else { panic(r) }
		}
	}()
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		e := dl.checkVersion(tr, false)
		if e != nil { return nil, e }

		if len(path) == 0 {
			return false, Error{"The root directory cannot be removed."}
		}

		node := dl.find(tr, path).prefetchMetadata(tr)

		if !node.exists() {
			return false, nil
		}

		if node.isInPartition(nil, true) {
			return node.getContents(dl, nil).DirectoryLayer().Remove(tr, node.getPartitionSubpath())
		}

		dl.removeRecursive(tr, node.subspace)
		dl.removeFromParent(tr, path)

		return true, nil
	})
	return r.(bool), e
}

func (dl DirectoryLayer) removeRecursive(tr fdb.Transaction, node subspace.Subspace) {
	nodes := dl.subdirNodes(tr, node)
	for i := 0; i < len(nodes); i++ {
		dl.removeRecursive(tr, nodes[i])
	}

	p, e := dl.nodeSS.Unpack(node.Key())
	if e != nil { panic(e) }
	kr, e := fdb.PrefixRange(p[0].([]byte)) // FIXME: or is it string?
	if e != nil { panic(e) }

	tr.ClearRange(kr)
	tr.ClearRange(node)
}

func (dl DirectoryLayer) removeFromParent(tr fdb.Transaction, path []string) {
	parent := dl.find(tr, path[:len(path)-1])
	tr.Clear(parent.subspace.Sub(SUBDIRS, path[len(path)-1]))
}

func (dl DirectoryLayer) CheckLayer(layer []byte) error {
	if layer != nil {
		return Error{"The directory was created with an incompatible layer."}
	}
	return nil
}

func (dl DirectoryLayer) subdirNames(tr fdb.Transaction, node subspace.Subspace) []string {
	sd := node.Sub(SUBDIRS)

	rr := tr.GetRange(sd, fdb.RangeOptions{})
	ri := rr.Iterator()

	var ret []string

	for ri.Advance() {
		kv := ri.GetNextOrPanic()

		p, e := sd.Unpack(kv.Key)
		if e != nil { panic(e) }

		ret = append(ret, p[0].(string))
	}

	return ret
}

func (dl DirectoryLayer) subdirNodes(tr fdb.Transaction, node subspace.Subspace) []subspace.Subspace {
	sd := node.Sub(SUBDIRS)

	rr := tr.GetRange(sd, fdb.RangeOptions{})
	ri := rr.Iterator()

	var ret []subspace.Subspace

	for ri.Advance() {
		kv := ri.GetNextOrPanic()

		ret = append(ret, dl.nodeWithPrefix(kv.Value))
	}

	return ret
}

// func (dl DirectoryLayer) subdirNodes(tr fdb.Transaction, node subspace.Subspace) []node {
// }

func (dl DirectoryLayer) nodeContainingKey(tr fdb.Transaction, key []byte) subspace.Subspace {
	if bytes.HasPrefix(key, dl.nodeSS.Key()) {
		return dl.rootNode
	}

	kr := fdb.KeyRange{dl.nodeSS.BeginKey(), fdb.Key(append(dl.nodeSS.Pack(tuple.Tuple{key}), 0x00))}

	kvs := tr.GetRange(kr, fdb.RangeOptions{Reverse:true, Limit:1}).GetSliceOrPanic()
	if len(kvs) == 1 {
		pp, e := dl.nodeSS.Unpack(kvs[0].Key)
		if e != nil {
			panic(e)
		}
		prevPrefix := pp[0].([]byte)
		if bytes.HasPrefix(key, prevPrefix) {
			return subspace.FromRawBytes(kvs[0].Key)
		}
	}

	return nil
}

func (dl DirectoryLayer) isPrefixFree(tr fdb.Transaction, prefix []byte) bool {
	if prefix == nil {
		return false
	}

	if dl.nodeContainingKey(tr, prefix) != nil {
		return false
	}

	kr, e := fdb.PrefixRange(prefix)
	if e != nil {
		// FIXME?
		panic(e)
	}

	kvs := tr.GetRange(fdb.KeyRange{fdb.Key(dl.nodeSS.Pack(tuple.Tuple{kr.BeginKey()})), fdb.Key(dl.nodeSS.Pack(tuple.Tuple{kr.EndKey()}))}, fdb.RangeOptions{Limit:1}).GetSliceOrPanic()
	if len(kvs) > 0 {
		return false
	}

	return true
}

func (dl DirectoryLayer) checkVersion(tr fdb.Transaction, writeAccess bool) error {
	version := tr.Get(dl.rootNode.Sub([]byte("version"))).GetOrPanic()

	if version == nil {
		if writeAccess {
			dl.initializeDirectory(tr)
		}
		return nil
	}

	var versions []int32
	buf := bytes.NewBuffer(version)

	for i := 0; i < 3; i++ {
		var v int32
		err := binary.Read(buf, binary.LittleEndian, &v)
		if err != nil {
			// FIXME: make our own error here
			return err
		}
		versions = append(versions, v)
	}

	if versions[0] > MAJORVERSION {
		return fmt.Errorf("Cannot load directory with version %d.%d.%d using directory layer %d.%d.%d", versions[0], versions[1], versions[2], MAJORVERSION, MINORVERSION, MICROVERSION)
	}

	if versions[1] > MINORVERSION && writeAccess {
		return fmt.Errorf("Directory with version %d.%d.%d is read-only when opened using directory layer %d.%d.%d", versions[0], versions[1], versions[2], MAJORVERSION, MINORVERSION, MICROVERSION)
	}

	return nil
}

func (dl DirectoryLayer) initializeDirectory(tr fdb.Transaction) {
	buf := new(bytes.Buffer)

	// FIXME: is ignoring errors OK here? What could really go wrong?
	binary.Write(buf, binary.LittleEndian, MAJORVERSION)
	binary.Write(buf, binary.LittleEndian, MINORVERSION)
	binary.Write(buf, binary.LittleEndian, MICROVERSION)

	tr.Set(dl.rootNode.Sub([]byte("version")), buf.Bytes())
}

func (dl DirectoryLayer) contentsOfNode(node subspace.Subspace, path []string, layer []byte) DirectorySubspace {
	p, err := dl.nodeSS.Unpack(node.Key())
	if err != nil {
		panic(err)
	}
	prefix := p[0]

	newPath := make([]string, len(dl.path) + len(path))
	copy(newPath, dl.path)
	copy(newPath[len(dl.path):], path)

	if bytes.Compare(layer, []byte("partition")) == 0 {
		// DP(dl.path + path, prefix, dl)
		return DirectoryPartition{}
	} else {
		return directorySubspace{subspace.FromRawBytes(prefix.([]byte)), directory{dl, newPath, layer}}
	}
}

func (dl DirectoryLayer) nodeWithPrefix(prefix []byte) subspace.Subspace {
	if prefix == nil {
		return nil
	}
	return dl.nodeSS.Sub(prefix)
}

func (dl DirectoryLayer) find(tr fdb.Transaction, path []string) *node {
	n := &node{dl.rootNode, []string{}, path, nil}
	for i := 0; i < len(path); i++ {
		n = &node{dl.nodeWithPrefix(tr.Get(n.subspace.Sub(SUBDIRS, path[i])).GetOrPanic()), path[:i+1], path, nil}
		if !n.exists() || bytes.Compare(n.layer(&tr).GetOrPanic(), []byte("partition")) == 0 {
			return n
		}
	}
	return n
}
