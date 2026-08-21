package lock

import (
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
	LockName string
	Id string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, Id: kvtest.RandValue(12), LockName: lockname}

	return lk
}

/*
	Acquire the lock for the given lockname.  Lock request is denied if 
	it is found that the lock is held by another.
*/
func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.LockName)
		if err == rpc.ErrNoKey {
			val, ver = "", 0
		}

		//already acquired
		if val == lk.Id {
			return
		}

		//someone else holds
		if val != "" {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		err = lk.ck.Put(lk.LockName, lk.Id, ver)
		if err == rpc.OK {
			return
		}

		//maybe put, try again
		if err == rpc.ErrMaybe {
			continue
		}
	}
}

func (lk *Lock) Release() {
	for {
		val, ver, err := lk.ck.Get(lk.LockName)
		//lock gone or acquired by someone else
		if err != rpc.OK || val != lk.Id {
			return
		}

		err = lk.ck.Put(lk.LockName, "", ver)
		if err == rpc.OK || err == rpc.ErrMaybe {
			return
		}
	}
}
