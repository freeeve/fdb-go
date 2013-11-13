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
	"math/rand"
)

var oneBytes = []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

type highContentionAllocator struct {
	counters, recent subspace.Subspace
}

func newHCA(s subspace.Subspace) highContentionAllocator {
	var hca highContentionAllocator

	hca.counters = s.Sub(0)
	hca.recent = s.Sub(1)

	return hca
}

func windowSize(start int64) int64 {
	// Larger window sizes are better for high contention, smaller sizes for
	// keeping the keys small.  But if there are many allocations, the keys
	// can't be too small.  So start small and scale up.  We don't want this to
	// ever get *too* big because we have to store about window_size/2 recent
	// items.
	if start < 255 { return 64 }
	if start < 65535 { return 1024 }
	return 8192
}

func (hca highContentionAllocator) allocate(t fdb.Transactor) []byte {
	ret, e := t.Transact(func (tr fdb.Transaction) (interface{}, error) {
		rr := tr.Snapshot().GetRange(hca.counters, fdb.RangeOptions{Limit:1, Reverse:true})
		kvs := rr.GetSliceOrPanic()

		var start, count int64

		if len(kvs) == 1 {
			t, e := hca.counters.Unpack(kvs[0].Key)
			if e != nil {
				return nil, e
			}
			start = t[0].(int64)

			e = binary.Read(bytes.NewBuffer(kvs[0].Value), binary.LittleEndian, &count)
			if e != nil {
				return nil, e
			}
		}

		window := windowSize(start)

		if (count + 1) * 2 >= window {
			// Advance the window
			// tr.ClearRange(hca.counters, append(subspaceAdd(hca.counters, start), 0x00))
			tr.ClearRange(fdb.KeyRange{hca.counters, append(hca.counters.Sub(start).ToFDBKey(), 0x00)})
			start += window
			// tr.ClearRange(hca.recent, subspaceAdd(hca.recent, start))
			tr.ClearRange(fdb.KeyRange{hca.recent, hca.recent.Sub(start)})
			window = windowSize(start)
		}

        // Increment the allocation count for the current window
		tr.Add(hca.counters.Sub(start), oneBytes)

		for {
            // As of the snapshot being read from, the window is less than half
            // full, so this should be expected to take 2 tries.  Under high
            // contention (and when the window advances), there is an additional
            // subsequent risk of conflict for this transaction.
			candidate := rand.Int63n(window) + start
			key := hca.recent.Sub(candidate)
			if tr.Get(key).GetOrPanic() == nil {
				tr.Set(key, []byte(""))
				return tuple.Tuple{candidate}.Pack(), nil
			}
		}
	})
	if e != nil { panic(e) }

	return ret.([]byte)
}
