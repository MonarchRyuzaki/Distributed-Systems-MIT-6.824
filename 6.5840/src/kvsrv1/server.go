package kvsrv

import (
	// "fmt"
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueTuple struct {
	value   string
	version int
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	KVStore map[string]ValueTuple
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		KVStore: make(map[string]ValueTuple),
	}
	kv.KVStore["l"] = ValueTuple{value: "", version: 1}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	valueTuple, ok := kv.KVStore[args.Key]
	// fmt.Printf("Enter KVServer Get with args Key:%s\n", args.Key)
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = valueTuple.value
	reply.Version = rpc.Tversion(valueTuple.version)
	reply.Err = rpc.OK
	// fmt.Printf("Exit KVServer Get with args Key:%s\n", args.Key)
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	valueTuple, ok := kv.KVStore[args.Key]
	// fmt.Printf("Enter KVServer Put with args Key:%s, Value:%s, Version:%d\n", args.Key, args.Value, args.Version)
	if !ok {
		if args.Version == 0 {
			kv.KVStore[args.Key] = ValueTuple{value: args.Value, version: 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return
	}
	if args.Version == rpc.Tversion(valueTuple.version) {
		kv.KVStore[args.Key] = ValueTuple{value: args.Value, version: valueTuple.version + 1}
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrVersion
	}
	// fmt.Printf("Exit KVServer Put with args Key:%s, Value:%s, Version:%d\n", args.Key, args.Value, args.Version)
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
