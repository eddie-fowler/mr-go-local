package kvsrv

import (
	"fmt"
	"time"

	types "6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)


type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, types.Tversion, types.Err) {
	// You will have to modify this function.
	args := types.GetArgs{Key: key}
	reply := types.GetReply{}

	var value string
	var version types.Tversion
	var err types.Err


	for ok := ck.clnt.Call(ck.server, "KVServer.Get", args, &reply); !ok; ok = ck.clnt.Call(ck.server, "KVServer.Get", args, &reply) {
		time.Sleep(100 * time.Millisecond)
	}

	switch reply.Err {
		case types.OK:
			value = reply.Value
			version = reply.Version
			err = types.OK
		case types.ErrNoKey:
			err = types.ErrNoKey
		default:
			fmt.Printf("Get failed for key: %v \n", key)
	}

	return 	value, version, err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version types.Tversion) types.Err {
	args := types.PutArgs{Key: key, Value: value, Version: version}
	reply := types.PutReply{}

	isRetried := false
	for ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply); !ok; ok = ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply){
		isRetried = true
		time.Sleep(100 * time.Millisecond)
	}

	//We don't know if the initial request was executed since [response] may have been dropped.
	//when a **request** is dropped we can more confidently retry since we know the server didn't execute 
	if isRetried && reply.Err == types.ErrVersion {
		reply.Err = types.ErrMaybe
	}

	return reply.Err
}
