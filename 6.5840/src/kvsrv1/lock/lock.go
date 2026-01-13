package lock

import (
	// "fmt"
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	clientId string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, clientId: kvtest.RandValue(8)}
	// fmt.Println("Making Lock Server and initializing clientId");
	// You may add code here
	return lk
}

func randRange(min, max int) int {
	return rand.Intn(max-min+1) + min
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		time.Sleep(time.Duration(randRange(100, 300)) * time.Millisecond)
		// fmt.Printf("Trying to get lock Status\n");
		value, version, err := lk.ck.Get("l")
		// fmt.Printf("Getting %v : (Value = %v, Version = %v, Error = %v) for client Id: %v\n", "l", value, version, err, lk.clientId);
		if value == lk.clientId {
			// fmt.Printf("The Lock is already taken by the user\n");
			return
		}
		if err != rpc.OK || value != "" {
			// fmt.Printf("The Lock is taken by some other client. Sleeping now\n")
			continue
		}
		// fmt.Printf("Putting %v : (Value = %v, Version = %v) for client Id: %v\n", "l", lk.clientId, version, lk.clientId);
		status := lk.ck.Put("l", lk.clientId, version)
		// fmt.Printf("Response for Put %v\n", status)
		if status == rpc.OK || status == rpc.ErrMaybe {
			// fmt.Printf("Granting lock to Client : %v\n", lk.clientId);
			return
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	_, version, _ := lk.ck.Get("l")
	lk.ck.Put("l", "", version)
}
