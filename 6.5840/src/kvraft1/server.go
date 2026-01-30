package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu      sync.Mutex
	KVStore map[string]ValueTuple
}

type ValueTuple struct {
	Value   string
	Version int
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) performGet(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	valueTuple, ok := kv.KVStore[args.Key]
	// fmt.Printf("Enter KVServer Get with args Key:%s\n", args.Key)
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = valueTuple.Value
	reply.Version = rpc.Tversion(valueTuple.Version)
	reply.Err = rpc.OK
}

func (kv *KVServer) performPut(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	valueTuple, ok := kv.KVStore[args.Key]
	// fmt.Printf("Enter KVServer Put with args Key:%s, Value:%s, Version:%d\n", args.Key, args.Value, args.Version)
	if !ok {
		if args.Version == 0 {
			kv.KVStore[args.Key] = ValueTuple{Value: args.Value, Version: 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return
	}
	if args.Version == rpc.Tversion(valueTuple.Version) {
		kv.KVStore[args.Key] = ValueTuple{Value: args.Value, Version: valueTuple.Version + 1}
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrVersion
	}
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch args := req.(type) {
	case rpc.GetArgs:
		reply := &rpc.GetReply{}
		kv.performGet(&args, reply)
		return reply
	case rpc.PutArgs:
		reply := &rpc.PutReply{}
		kv.performPut(&args, reply)
		return reply
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.KVStore)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if data == nil || len(data) < 1 {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if d.Decode(&kv.KVStore) != nil {
		log.Fatal("Failed to decode KVStore from snapshot")
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, res := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return
	}
	data, ok := res.(*rpc.GetReply)
	if !ok {
		log.Fatalf("Type Mismatch in kvserver Get")
	}
	*reply = *data
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, res := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return
	}
	data, ok := res.(*rpc.PutReply)
	if !ok {
		log.Fatalf("Type Mismatch in kvserver Put")
	}
	*reply = *data
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(ValueTuple{})
	labgob.Register(map[string]ValueTuple{})

	kv := &KVServer{me: me, KVStore: make(map[string]ValueTuple)}
	kv.KVStore["l"] = ValueTuple{Value: "", Version: 1}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
