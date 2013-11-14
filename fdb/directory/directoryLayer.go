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
	"encoding/binary"
	"bytes"
	"fmt"
	"errors"
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

	dl.rootNode = dl.nodeSS.Sub(dl.nodeSS.Bytes())
	dl.allocator = newHCA(dl.rootNode.Sub([]byte("hca")))

	dl.path = path

	return dl
}

func (dl DirectoryLayer) getLayerForPath(path []string) DirectoryLayer {
	return dl
}

func (dl DirectoryLayer) createOrOpen(rtr fdb.ReadTransaction, tr *fdb.Transaction, path []string, layer []byte, prefix []byte, allowCreate, allowOpen bool) (DirectorySubspace, error) {
	if e := dl.checkVersion(rtr, nil); e != nil {
		return nil, e
	}

	if len(path) == 0 {
		return nil, errors.New("the root directory cannot be opened")
	}

	existingNode := dl.find(rtr, path).prefetchMetadata(rtr)
	if existingNode.exists() {
		if existingNode.isInPartition(nil, false) {
			subpath := existingNode.getPartitionSubpath()
			enc, e := existingNode.getContents(dl, nil)
			if e != nil {
				return nil, e
			}
			return enc.(DirectoryPartition).createOrOpen(rtr, tr, subpath, layer, prefix, allowCreate, allowOpen)
		}

		if !allowOpen {
			return nil, errors.New("the directory already exists")
		}

		if layer != nil && bytes.Compare(existingNode._layer.GetOrPanic(), layer) != 0 {
			return nil, errors.New("the directory was created with an incompatible layer")
		}

		return existingNode.getContents(dl, nil)
	}

	if !allowCreate {
		return nil, errors.New("the directory does not exist")
	}

	if e := dl.checkVersion(rtr, tr); e != nil {
		return nil, e
	}

	if prefix == nil {
		newss, e := dl.allocator.allocate(*tr, dl.contentSS)
		if e != nil {
			return nil, fmt.Errorf("unable to allocate new directory prefix (%s)", e.Error())
		}
		prefix = newss.Bytes()
	}

	pf, e := dl.isPrefixFree(*tr, prefix)
	if e != nil {
		return nil, e
	}
	if !pf {
		return nil, errors.New("the given prefix is already in use")
	}

	var parentNode subspace.Subspace

	if len(path) > 1 {
		pd, e := dl.createOrOpen(rtr, tr, path[:len(path)-1], nil, nil, true, true)
		if e != nil {
			return nil, e
		}
		parentNode = dl.nodeWithPrefix(pd.Bytes())
	} else {
		parentNode = dl.rootNode
	}

	if parentNode == nil {
		return nil, errors.New("the parent directory does not exist")
	}

	node := dl.nodeWithPrefix(prefix)
	tr.Set(parentNode.Sub(_SUBDIRS, path[len(path)-1]), prefix)

	if layer == nil {
		layer = []byte{}
	}

	tr.Set(node.Sub([]byte("layer")), layer)

	return dl.contentsOfNode(node, path, layer)
}

func (dl DirectoryLayer) CreateOrOpen(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		return dl.createOrOpen(tr, &tr, path, layer, nil, true, true)
	})
	if e != nil {
		return nil, e
	}
	return r.(DirectorySubspace), nil
}

func (dl DirectoryLayer) Create(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		return dl.createOrOpen(tr, &tr, path, layer, nil, true, false)
	})
	if e != nil {
		return nil, e
	}
	return r.(DirectorySubspace), nil
}

func (dl DirectoryLayer) CreatePrefix(t fdb.Transactor, path []string, layer []byte, prefix []byte) (DirectorySubspace, error) {
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		return dl.createOrOpen(tr, &tr, path, layer, prefix, true, false)
	})
	if e != nil {
		return nil, e
	}
	return r.(DirectorySubspace), nil
}

func (dl DirectoryLayer) Open(rt fdb.ReadTransactor, path []string, layer []byte) (DirectorySubspace, error) {
	r, e := rt.ReadTransact(func (sn fdb.Snapshot) (interface{}, error) {
		return dl.createOrOpen(sn, nil, path, layer, nil, false, true)
	})
	if e != nil {
		return nil, e
	}
	return r.(DirectorySubspace), nil
}

func (dl DirectoryLayer) Exists(rt fdb.ReadTransactor, path []string) (bool, error) {
	r, e := rt.ReadTransact(func (sn fdb.Snapshot) (interface{}, error) {
		if e := dl.checkVersion(sn, nil); e != nil {
			return false, e
		}

		node := dl.find(sn, path).prefetchMetadata(sn)
		if !node.exists() {
			return false, nil
		}

		if node.isInPartition(nil, false) {
			nc, e := node.getContents(dl, nil)
			if e != nil {
				return false, e
			}
			return nc.Exists(sn, node.getPartitionSubpath())
		}

		return true, nil
	})
	return r.(bool), e
}

func (dl DirectoryLayer) List(rt fdb.ReadTransactor, path []string) ([]string, error) {
	r, e := rt.ReadTransact(func (sn fdb.Snapshot) (interface{}, error) {
		if e := dl.checkVersion(sn, nil); e != nil {
			return nil, e
		}

		node := dl.find(sn, path).prefetchMetadata(sn)
		if !node.exists() {
			return nil, errors.New("the directory does not exist")
		}

		if node.isInPartition(nil, true) {
			nc, e := node.getContents(dl, nil)
			if e != nil {
				return nil, e
			}
			return nc.List(sn, node.getPartitionSubpath())
		}

		return dl.subdirNames(sn, node.subspace)
	})

	return r.([]string), e
}

func (dl DirectoryLayer) MoveTo(t fdb.Transactor, newAbsolutePath []string) (DirectorySubspace, error) {
	return nil, errors.New("the root directory cannot be moved")
}

func (dl DirectoryLayer) Move(t fdb.Transactor, oldPath []string, newPath []string) (DirectorySubspace, error) {
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		if e := dl.checkVersion(tr, nil); e != nil {
			return nil, e
		}

		sliceEnd := len(oldPath)
		if sliceEnd > len(newPath) {
			sliceEnd = len(newPath)
		}
		if stringsEqual(oldPath, newPath[:sliceEnd]) {
			return nil, errors.New("the destination directory cannot be a subdirectory of the source directory")
		}

		oldNode := dl.find(tr, oldPath).prefetchMetadata(tr)
		newNode := dl.find(tr, newPath).prefetchMetadata(tr)

		if !oldNode.exists() {
			return nil, errors.New("the source directory does not exist")
		}

		if oldNode.isInPartition(nil, false) || newNode.isInPartition(nil, false) {
			if !oldNode.isInPartition(nil, false) || !newNode.isInPartition(nil, false) || !stringsEqual(oldNode.path, newNode.path) {
				return nil, errors.New("cannot move between partitions")
			}

			nnc, e := newNode.getContents(dl, nil)
			if e != nil {
				return nil, e
			}
			return nnc.Move(tr, oldNode.getPartitionSubpath(), newNode.getPartitionSubpath())
		}

		if newNode.exists() {
			return nil, errors.New("the destination directory already exists. Remove it first")
		}

		parentNode := dl.find(tr, newPath[:len(newPath)-1])
		if !parentNode.exists() {
			return nil, errors.New("the parent of the destination directory does not exist. Create it first")
		}

		p, e := dl.nodeSS.Unpack(oldNode.subspace)
		if e != nil {
			return nil, e
		}
		tr.Set(parentNode.subspace.Sub(_SUBDIRS, newPath[len(newPath)-1]), p[0].([]byte))

		dl.removeFromParent(tr, oldPath)

		return dl.contentsOfNode(oldNode.subspace, newPath, oldNode._layer.GetOrPanic())
	})

	if e != nil {
		return nil, e
	}
	return r.(DirectorySubspace), nil
}

func (dl DirectoryLayer) Remove(t fdb.Transactor, path []string) (bool, error) {
	r, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		if e := dl.checkVersion(tr, nil); e != nil {
			return false, e
		}

		if len(path) == 0 {
			return false, errors.New("the root directory cannot be removed")
		}

		node := dl.find(tr, path).prefetchMetadata(tr)

		if !node.exists() {
			return false, nil
		}

		if node.isInPartition(nil, true) {
			nc, e := node.getContents(dl, nil)
			if e != nil {
				return false, e
			}
			return nc.(DirectoryPartition).Remove(tr, node.getPartitionSubpath())
		}

		if e := dl.removeRecursive(tr, node.subspace); e != nil {
			return false, e
		}
		dl.removeFromParent(tr, path)

		return true, nil
	})
	return r.(bool), e
}

func (dl DirectoryLayer) removeRecursive(tr fdb.Transaction, node subspace.Subspace) error {
	nodes := dl.subdirNodes(tr, node)
	for i := 0; i < len(nodes); i++ {
		if e := dl.removeRecursive(tr, nodes[i]); e != nil {
			return e
		}
	}

	p, e := dl.nodeSS.Unpack(node)
	if e != nil { return e }
	kr, e := fdb.PrefixRange(p[0].([]byte))
	if e != nil { return e }

	tr.ClearRange(kr)
	tr.ClearRange(node)

	return nil
}

func (dl DirectoryLayer) removeFromParent(tr fdb.Transaction, path []string) {
	parent := dl.find(tr, path[:len(path)-1])
	tr.Clear(parent.subspace.Sub(_SUBDIRS, path[len(path)-1]))
}

func (dl DirectoryLayer) GetLayer() []byte {
	return []byte{}
}

func (dl DirectoryLayer) GetPath() []string {
	return dl.path
}

func (dl DirectoryLayer) subdirNames(rtr fdb.ReadTransaction, node subspace.Subspace) ([]string, error) {
	sd := node.Sub(_SUBDIRS)

	rr := rtr.GetRange(sd, fdb.RangeOptions{})
	ri := rr.Iterator()

	var ret []string

	for ri.Advance() {
		kv := ri.GetNextOrPanic()

		p, e := sd.Unpack(kv.Key)
		if e != nil {
			return nil, e
		}

		ret = append(ret, p[0].(string))
	}

	return ret, nil
}

func (dl DirectoryLayer) subdirNodes(tr fdb.Transaction, node subspace.Subspace) []subspace.Subspace {
	sd := node.Sub(_SUBDIRS)

	rr := tr.GetRange(sd, fdb.RangeOptions{})
	ri := rr.Iterator()

	var ret []subspace.Subspace

	for ri.Advance() {
		kv := ri.GetNextOrPanic()

		ret = append(ret, dl.nodeWithPrefix(kv.Value))
	}

	return ret
}

func (dl DirectoryLayer) nodeContainingKey(tr fdb.Transaction, key []byte) (subspace.Subspace, error) {
	if bytes.HasPrefix(key, dl.nodeSS.Bytes()) {
		return dl.rootNode, nil
	}

	kr := fdb.KeyRange{dl.nodeSS.BeginKey(), fdb.Key(append(dl.nodeSS.Pack(tuple.Tuple{key}), 0x00))}

	kvs := tr.GetRange(kr, fdb.RangeOptions{Reverse:true, Limit:1}).GetSliceOrPanic()
	if len(kvs) == 1 {
		pp, e := dl.nodeSS.Unpack(kvs[0].Key)
		if e != nil {
			return nil, e
		}
		prevPrefix := pp[0].([]byte)
		if bytes.HasPrefix(key, prevPrefix) {
			return subspace.FromBytes(kvs[0].Key), nil
		}
	}

	return nil, nil
}

func (dl DirectoryLayer) isPrefixFree(tr fdb.Transaction, prefix []byte) (bool, error) {
	if prefix == nil {
		return false, nil
	}

	nck, e := dl.nodeContainingKey(tr, prefix)
	if e != nil {
		return false, e
	}
	if nck != nil {
		return false, nil
	}

	kr, e := fdb.PrefixRange(prefix)
	if e != nil {
		return false, e
	}

	kvs := tr.GetRange(fdb.KeyRange{fdb.Key(dl.nodeSS.Pack(tuple.Tuple{kr.BeginKey()})), fdb.Key(dl.nodeSS.Pack(tuple.Tuple{kr.EndKey()}))}, fdb.RangeOptions{Limit:1}).GetSliceOrPanic()
	if len(kvs) > 0 {
		return false, nil
	}

	return true, nil
}

func (dl DirectoryLayer) checkVersion(rtr fdb.ReadTransaction, tr *fdb.Transaction) error {
	version := rtr.Get(dl.rootNode.Sub([]byte("version"))).GetOrPanic()

	if version == nil {
		if tr != nil {
			dl.initializeDirectory(*tr)
		}
		return nil
	}

	var versions []int32
	buf := bytes.NewBuffer(version)

	for i := 0; i < 3; i++ {
		var v int32
		err := binary.Read(buf, binary.LittleEndian, &v)
		if err != nil {
			return errors.New("cannot determine directory version present in database")
		}
		versions = append(versions, v)
	}

	if versions[0] > _MAJORVERSION {
		return fmt.Errorf("cannot load directory with version %d.%d.%d using directory layer %d.%d.%d", versions[0], versions[1], versions[2], _MAJORVERSION, _MINORVERSION, _MICROVERSION)
	}

	if versions[1] > _MINORVERSION && tr != nil /* aka write access allowed */ {
		return fmt.Errorf("directory with version %d.%d.%d is read-only when opened using directory layer %d.%d.%d", versions[0], versions[1], versions[2], _MAJORVERSION, _MINORVERSION, _MICROVERSION)
	}

	return nil
}

func (dl DirectoryLayer) initializeDirectory(tr fdb.Transaction) {
	buf := new(bytes.Buffer)

	// FIXME: is ignoring errors OK here? What could really go wrong?
	binary.Write(buf, binary.LittleEndian, _MAJORVERSION)
	binary.Write(buf, binary.LittleEndian, _MINORVERSION)
	binary.Write(buf, binary.LittleEndian, _MICROVERSION)

	tr.Set(dl.rootNode.Sub([]byte("version")), buf.Bytes())
}

func (dl DirectoryLayer) contentsOfNode(node subspace.Subspace, path []string, layer []byte) (DirectorySubspace, error) {
	p, e := dl.nodeSS.Unpack(node)
	if e != nil {
		return nil, e
	}
	prefix := p[0]

	newPath := make([]string, len(dl.path) + len(path))
	copy(newPath, dl.path)
	copy(newPath[len(dl.path):], path)

	pb := prefix.([]byte)
	ss := subspace.FromBytes(pb)

	if bytes.Compare(layer, []byte("partition")) == 0 {
		nssb := make([]byte, len(pb) + 1)
		copy(nssb, pb)
		nssb[len(pb)] = 0xFE
		return DirectoryPartition{NewDirectoryLayer(subspace.FromBytes(nssb), ss, newPath), dl}, nil
	} else {
		return directorySubspace{ss, dl, newPath, layer}, nil
	}
}

func (dl DirectoryLayer) nodeWithPrefix(prefix []byte) subspace.Subspace {
	if prefix == nil { return nil }
	return dl.nodeSS.Sub(prefix)
}

func (dl DirectoryLayer) find(rtr fdb.ReadTransaction, path []string) *node {
	n := &node{dl.rootNode, []string{}, path, nil}
	for i := 0; i < len(path); i++ {
		n = &node{dl.nodeWithPrefix(rtr.Get(n.subspace.Sub(_SUBDIRS, path[i])).GetOrPanic()), path[:i+1], path, nil}
		if !n.exists() || bytes.Compare(n.layer(rtr).GetOrPanic(), []byte("partition")) == 0 {
			return n
		}
	}
	return n
}

func (dl DirectoryLayer) partitionSubpath(lpath, rpath []string) []string {
	r := make([]string, len(lpath) - len(dl.path) + len(rpath))
	copy(r, lpath[len(dl.path):])
	copy(r[len(lpath) - len(dl.path):], rpath)
	return r
}
