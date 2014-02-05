package main

import (
	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/FoundationDB/fdb-go/fdb/tuple"
	"log"
	"fmt"
	"os"
	"strings"
	"sync"
	"runtime"
	"reflect"
)

const verbose bool = false

func int64ToBool(i int64) bool {
	switch i {
	case 0:
		return false
	default:
		return true
	}
}

type stackEntry struct {
	item interface{}
	idx int
}

type StackMachine struct {
	prefix []byte
	tr fdb.Transaction
	stack []stackEntry
	lastVersion int64
	threads sync.WaitGroup
	verbose bool
	de *DirectoryExtension
}

func newStackMachine(prefix []byte, verbose bool, de *DirectoryExtension) *StackMachine {
	sm := StackMachine{verbose: verbose, prefix: prefix, de: de}
	return &sm
}

func (sm *StackMachine) waitAndPop() (ret stackEntry) {
	defer func() {
		if r := recover(); r != nil {
			switch r := r.(type) {
			case fdb.Error:
				ret.item = []byte(tuple.Tuple{[]byte("ERROR"), []byte(fmt.Sprintf("%d", r.Code))}.Pack())
			default:
				panic(r)
			}
		}
	}()

	ret, sm.stack = sm.stack[len(sm.stack) - 1], sm.stack[:len(sm.stack) - 1]
	switch el := ret.item.(type) {
	case int64, []byte, string:
	case fdb.Key:
		ret.item = []byte(el)
	case fdb.FutureNil:
		el.MustGet()
		ret.item = []byte("RESULT_NOT_PRESENT")
	case fdb.FutureByteSlice:
		v := el.MustGet()
		if v != nil {
			ret.item = v
		} else {
			ret.item = []byte("RESULT_NOT_PRESENT")
		}
	case fdb.FutureKey:
		ret.item = []byte(el.MustGet())
	case nil:
	default:
		log.Fatalf("Don't know how to pop stack element %v %T\n", el, el)
	}
	return
}

func (sm *StackMachine) popSelector() fdb.KeySelector {
	sel := fdb.KeySelector{fdb.Key(sm.waitAndPop().item.([]byte)), int64ToBool(sm.waitAndPop().item.(int64)), int(sm.waitAndPop().item.(int64))}
	return sel
}

func (sm *StackMachine) popKeyRange() fdb.KeyRange {
	kr := fdb.KeyRange{fdb.Key(sm.waitAndPop().item.([]byte)), fdb.Key(sm.waitAndPop().item.([]byte))}
	return kr
}

func (sm *StackMachine) popRangeOptions() fdb.RangeOptions {
	var limit int
	switch l := sm.waitAndPop().item.(type) {
	case int64:
		limit = int(l)
	}
	ro := fdb.RangeOptions{Limit: limit, Reverse: int64ToBool(sm.waitAndPop().item.(int64)), Mode: fdb.StreamingMode(sm.waitAndPop().item.(int64) + 1)}
	return ro
}

func (sm *StackMachine) popPrefixRange() fdb.ExactRange {
	er, e := fdb.PrefixRange(sm.waitAndPop().item.([]byte))
	if e != nil {
		panic(e)
	}
	return er
}

func (sm *StackMachine) pushRange(idx int, sl []fdb.KeyValue) {
	var t tuple.Tuple = make(tuple.Tuple, 0, len(sl) * 2)

	for _, kv := range(sl) {
		t = append(t, kv.Key)
		t = append(t, kv.Value)
	}

	sm.store(idx, []byte(t.Pack()))
}

func (sm *StackMachine) store(idx int, item interface{}) {
	sm.stack = append(sm.stack, stackEntry{item, idx})
}

func (sm *StackMachine) dumpStack() {
	for i := len(sm.stack) - 1; i >= 0; i-- {
		el := sm.stack[i].item
		switch el := el.(type) {
		case int64:
			fmt.Printf(" %d", el)
		case fdb.FutureNil:
			fmt.Printf(" FutureNil")
		case fdb.FutureByteSlice:
			fmt.Printf(" FutureByteSlice")
		case fdb.FutureKey:
			fmt.Printf(" FutureKey")
		case []byte:
			fmt.Printf(" %q", string(el))
		case string:
			fmt.Printf(" %s", el)
		case nil:
			fmt.Printf(" nil")
		default:
			log.Fatalf("Don't know how to dump stack element %v %T\n", el, el)
		}
	}
}

func (sm *StackMachine) executeMutation(t fdb.Transactor, f func (fdb.Transaction) (interface{}, error), isDB bool, idx int) {
	_, e := t.Transact(f)
	if e != nil {
		panic(e)
	}
	if isDB {
		sm.store(idx, []byte("RESULT_NOT_PRESENT"))
	}
}

func (sm *StackMachine) processInst(idx int, inst tuple.Tuple) {
	defer func() {
		if r := recover(); r != nil {
			switch r := r.(type) {
			case fdb.Error:
				sm.store(idx, []byte(tuple.Tuple{[]byte("ERROR"), []byte(fmt.Sprintf("%d", r.Code))}.Pack()))
			default:
				panic(r)
			}
		}
	}()

	var e error

	op := string(inst[0].([]byte))
	if sm.verbose {
		fmt.Printf("%d. Instruction is %s (%v)\n", idx, op, sm.prefix)
		fmt.Printf("Stack from [")
		sm.dumpStack()
		fmt.Printf(" ]\n")
	}

	var t fdb.Transactor
	var rt fdb.ReadTransactor

	var isDB bool

	switch {
	case strings.HasSuffix(op, "_SNAPSHOT"):
		rt = sm.tr.Snapshot()
		op = op[:len(op)-9]
	case strings.HasSuffix(op, "_DATABASE"):
		t = db
		rt = db
		op = op[:len(op)-9]
		isDB = true
	default:
		t = sm.tr
		rt = sm.tr
	}

	switch {
	case op == "PUSH":
		sm.store(idx, inst[1])
	case op == "DUP":
		entry := sm.stack[len(sm.stack) - 1]
		sm.store(entry.idx, entry.item)
	case op == "EMPTY_STACK":
		sm.stack = []stackEntry{}
		sm.stack = make([]stackEntry, 0)
	case op == "SWAP":
		idx := sm.waitAndPop().item.(int64)
		sm.stack[len(sm.stack) - 1], sm.stack[len(sm.stack) - 1 - int(idx)] = sm.stack[len(sm.stack) - 1 - int(idx)], sm.stack[len(sm.stack) - 1]
	case op == "POP":
		sm.stack = sm.stack[:len(sm.stack) - 1]
	case op == "SUB":
		sm.store(idx, sm.waitAndPop().item.(int64) - sm.waitAndPop().item.(int64))
	case op == "NEW_TRANSACTION":
		sm.tr, e = db.CreateTransaction()
		if e != nil {
			panic(e)
		}
	case op == "ON_ERROR":
		sm.store(idx, sm.tr.OnError(fdb.Error{int(sm.waitAndPop().item.(int64))}))
	case op == "GET_READ_VERSION":
		_, e = rt.ReadTransact(func (rtr fdb.ReadTransaction) (interface{}, error) {
			sm.lastVersion = rtr.GetReadVersion().MustGet()
			sm.store(idx, []byte("GOT_READ_VERSION"))
			return nil, nil
		})
		if e != nil { panic(e) }
	case op == "SET":
		sm.executeMutation(t, func (tr fdb.Transaction) (interface{}, error) {
			tr.Set(fdb.Key(sm.waitAndPop().item.([]byte)), sm.waitAndPop().item.([]byte))
			return nil, nil
		}, isDB, idx)
	case op == "LOG_STACK":
		prefix := sm.waitAndPop().item.([]byte)
		for i := len(sm.stack)-1; i >= 0; i-- {
			if i % 100 == 0 {
				sm.tr.Commit().MustGet()
			}

			el := sm.waitAndPop()

			var keyt tuple.Tuple
			keyt = append(keyt, int64(i))
			keyt = append(keyt, int64(el.idx))
			pk := append(prefix, keyt.Pack()...)

			var valt tuple.Tuple
			valt = append(valt, el.item)
			pv := valt.Pack()

			vl := 40000
			if len(pv) < vl {
				vl = len(pv)
			}

			sm.tr.Set(fdb.Key(pk), pv[:vl])
		}
		sm.tr.Commit().MustGet()
	case op == "GET":
		_, e = rt.ReadTransact(func (rtr fdb.ReadTransaction) (interface{}, error) {
			sm.store(idx, rtr.Get(fdb.Key(sm.waitAndPop().item.([]byte))))
			return nil, nil
		})
		if e != nil { panic(e) }
	case op == "COMMIT":
		sm.store(idx, sm.tr.Commit())
	case op == "RESET":
		sm.tr.Reset()
	case op == "CLEAR":
		sm.executeMutation(t, func (tr fdb.Transaction) (interface{}, error) {
			tr.Clear(fdb.Key(sm.waitAndPop().item.([]byte)))
			return nil, nil
		}, isDB, idx)
	case op == "SET_READ_VERSION":
		sm.tr.SetReadVersion(sm.lastVersion)
	case op == "WAIT_FUTURE":
		entry := sm.waitAndPop()
		sm.store(entry.idx, entry.item)
	case op == "GET_COMMITTED_VERSION":
		sm.lastVersion, e = sm.tr.GetCommittedVersion()
		if e != nil {
			panic(e)
		}
		sm.store(idx, []byte("GOT_COMMITTED_VERSION"))
	case op == "GET_KEY":
		sel := sm.popSelector()
		_, e = rt.ReadTransact(func (rtr fdb.ReadTransaction) (interface{}, error) {
			sm.store(idx, rtr.GetKey(sel))
			return nil, nil
		})
		if e != nil { panic(e) }
	case strings.HasPrefix(op, "GET_RANGE"):
		var r fdb.Range

		switch op[9:] {
		case "_STARTS_WITH":
			r = sm.popPrefixRange()
		case "_SELECTOR":
			r = fdb.SelectorRange{sm.popSelector(), sm.popSelector()}
		case "":
			r = sm.popKeyRange()
		}

		ro := sm.popRangeOptions()
		_, e = rt.ReadTransact(func (rtr fdb.ReadTransaction) (interface{}, error) {
			sm.pushRange(idx, rtr.GetRange(r, ro).GetSliceOrPanic())
			return nil, nil
		})
		if e != nil { panic(e) }
	case strings.HasPrefix(op, "CLEAR_RANGE"):
		var er fdb.ExactRange

		switch op[11:] {
		case "_STARTS_WITH":
			er = sm.popPrefixRange()
		case "":
			er = sm.popKeyRange()
		}

		sm.executeMutation(t, func (tr fdb.Transaction) (interface{}, error) {
			tr.ClearRange(er)
			return nil, nil
		}, isDB, idx)
	case op == "TUPLE_PACK":
		var t tuple.Tuple
		count := sm.waitAndPop().item.(int64)
		for i := 0; i < int(count); i++ {
			t = append(t, sm.waitAndPop().item)
		}
		sm.store(idx, []byte(t.Pack()))
	case op == "TUPLE_UNPACK":
		t, e := tuple.Unpack(fdb.Key(sm.waitAndPop().item.([]byte)))
		if e != nil {
			panic(e)
		}
		for _, el := range(t) {
			sm.store(idx, []byte(tuple.Tuple{el}.Pack()))
		}
	case op == "TUPLE_RANGE":
		var t tuple.Tuple
		count := sm.waitAndPop().item.(int64)
		for i := 0; i < int(count); i++ {
			t = append(t, sm.waitAndPop().item)
		}
		bk, ek := t.FDBRangeKeys()
		sm.store(idx, []byte(bk.FDBKey()))
		sm.store(idx, []byte(ek.FDBKey()))
	case op == "START_THREAD":
		newsm := newStackMachine(sm.waitAndPop().item.([]byte), verbose, sm.de)
		sm.threads.Add(1)
		go func() {
			newsm.Run()
			sm.threads.Done()
		}()
	case op == "WAIT_EMPTY":
		prefix := sm.waitAndPop().item.([]byte)
		er, e := fdb.PrefixRange(prefix)
		if e != nil {
			panic(e)
		}
		db.Transact(func (tr fdb.Transaction) (interface{}, error) {
			v := tr.GetRange(er, fdb.RangeOptions{}).GetSliceOrPanic()
			if len(v) != 0 {
				panic(fdb.Error{1020})
			}
			return nil, nil
		})
		sm.store(idx, []byte("WAITED_FOR_EMPTY"))
	case op == "READ_CONFLICT_RANGE":
		e = sm.tr.AddReadConflictRange(fdb.KeyRange{fdb.Key(sm.waitAndPop().item.([]byte)), fdb.Key(sm.waitAndPop().item.([]byte))})
		if e != nil {
			panic(e)
		}
		sm.store(idx, []byte("SET_CONFLICT_RANGE"))
	case op == "WRITE_CONFLICT_RANGE":
		e = sm.tr.AddWriteConflictRange(fdb.KeyRange{fdb.Key(sm.waitAndPop().item.([]byte)), fdb.Key(sm.waitAndPop().item.([]byte))})
		if e != nil {
			panic(e)
		}
		sm.store(idx, []byte("SET_CONFLICT_RANGE"))
	case op == "READ_CONFLICT_KEY":
		e = sm.tr.AddReadConflictKey(fdb.Key(sm.waitAndPop().item.([]byte)))
		if e != nil {
			panic(e)
		}
		sm.store(idx, []byte("SET_CONFLICT_KEY"))
	case op == "WRITE_CONFLICT_KEY":
		e = sm.tr.AddWriteConflictKey(fdb.Key(sm.waitAndPop().item.([]byte)))
		if e != nil {
			panic(e)
		}
		sm.store(idx, []byte("SET_CONFLICT_KEY"))
	case op == "ATOMIC_OP":
		opname := strings.Replace(strings.Title(strings.Replace(strings.ToLower(string(sm.waitAndPop().item.([]byte))), "_", " ", -1)), " ", "", -1)
		key := fdb.Key(sm.waitAndPop().item.([]byte))
		value := sm.waitAndPop().item.([]byte)
		sm.executeMutation(t, func (tr fdb.Transaction) (interface{}, error) {
			reflect.ValueOf(tr).MethodByName(opname).Call([]reflect.Value{reflect.ValueOf(key), reflect.ValueOf(value)})
			return nil, nil
		}, isDB, idx)
	case op == "DISABLE_WRITE_CONFLICT":
		sm.tr.Options().SetNextWriteNoWriteConflictRange()
	case op == "CANCEL":
		sm.tr.Cancel()
	case op == "UNIT_TESTS":
	case strings.HasPrefix(op, "DIRECTORY_"):
		sm.de.processOp(sm, op[10:], isDB, idx, t, rt)
	default:
		log.Fatalf("Unhandled operation %s\n", string(inst[0].([]byte)))
	}

	if sm.verbose {
		fmt.Printf("        to [")
		sm.dumpStack()
		fmt.Printf(" ]\n\n")
	}

	runtime.Gosched()
}

func (sm *StackMachine) Run() {
	r, e := db.Transact(func (tr fdb.Transaction) (interface{}, error) {
		return tr.GetRange(tuple.Tuple{sm.prefix}, fdb.RangeOptions{}).GetSliceOrPanic(), nil
	})
	if e != nil {
		panic(e)
	}

	instructions := r.([]fdb.KeyValue)

	for i, kv := range(instructions) {
		inst, _ := tuple.Unpack(fdb.Key(kv.Value))

		if sm.verbose {
			fmt.Printf("Instruction %d\n", i)
		}
		sm.processInst(i, inst)
	}

	sm.threads.Wait()
}

var db fdb.Database

func main() {
	var clusterFile string

	prefix := []byte(os.Args[1])
	if len(os.Args) > 2 {
		clusterFile = os.Args[2]
	}

	var e error

	e = fdb.APIVersion(200)
	if e != nil {
		log.Fatal(e)
	}

	db, e = fdb.Open(clusterFile, []byte("DB"))
	if e != nil {
		log.Fatal(e)
	}

	sm := newStackMachine(prefix, verbose, newDirectoryExtension())

	sm.Run()
}
