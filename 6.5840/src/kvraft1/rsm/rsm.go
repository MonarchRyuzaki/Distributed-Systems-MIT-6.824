package rsm

import (
	"math/rand/v2"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	ID  int
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	pendingOps map[int]chan any
	dead       chan struct{}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pendingOps:   make(map[int]chan any),
		dead:         make(chan struct{}),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if !ok {
				continue
			}

			val := rsm.sm.DoOp(op.Req)

			rsm.mu.Lock()
			ch, exists := rsm.pendingOps[op.ID]
			if exists {
				delete(rsm.pendingOps, op.ID)
				rsm.mu.Unlock()
				ch <- val
			} else {
				rsm.mu.Unlock()
			}
		}
	}
	close(rsm.dead)
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
//
// IMPORTANT: Why we use unique IDs and per-operation channels (pendingOps map)
// ---------------------------------------------------------------------------
// Problem: Without ID matching, a leader could receive responses for operations
// it didn't submit. Consider this scenario:
//
//   1. Term 1: Server A is leader, submits op1 at index 5
//   2. Network partition: Server A gets isolated
//   3. Term 2: Server B becomes leader, commits op2 at index 5 (overwrites op1)
//   4. Partition heals: Server A rejoins
//   5. Term 3: Server A becomes leader again
//
// Now Server A's reader() receives the commit for index 5, but it contains op2
// (not op1). Without ID matching, Server A would return op2's result to the
// client that submitted op1 - returning the wrong response!
//
// Solution: Each operation gets a unique ID. The reader() only sends responses
// to Submit() calls that are waiting for that specific ID. If the ID doesn't
// match any pending operation, the response is discarded (it was for a different
// server's operation or an operation that already timed out).
//
// Why check for term change?
// --------------------------
// If the term changes (even if we're still leader), our operation might have
// been overwritten by a leader in an intermediate term. We must return
// ErrWrongLeader so the client retries, ensuring the operation actually commits.

func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	term, isLeader := rsm.Raft().GetState()
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	op := Op{Me: rsm.me, ID: rand.Int(), Req: req}

	responseCh := make(chan any, 1)

	rsm.mu.Lock()
	rsm.pendingOps[op.ID] = responseCh
	rsm.mu.Unlock()

	_, _, ok := rsm.Raft().Start(op)
	if !ok {
		rsm.mu.Lock()
		delete(rsm.pendingOps, op.ID)
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	for {
		select {
		case val := <-responseCh:
			return rpc.OK, val
		case <-rsm.dead:
			return rpc.ErrWrongLeader, nil
		case <-time.After(10 * time.Millisecond):
			currentTerm, isLeader := rsm.Raft().GetState()
			if !isLeader || currentTerm != term {
				rsm.mu.Lock()
				delete(rsm.pendingOps, op.ID)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		}

	}

	return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}
