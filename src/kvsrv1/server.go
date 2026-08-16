package kvsrv

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"

	types "6.5840/kvsrv1/rpc"
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

type KVServer struct {
	mu sync.Mutex
	KeyValueItems map[string]KeyValueItem
}

type KeyValueItem struct {
	Key string
	Value string
	Version types.Tversion
}

func MakeKVServer(sockname string) *KVServer {
	kv := &KVServer{}
	kv.KeyValueItems = make(map[string]KeyValueItem)

	rpc.Register(kv)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *types.GetArgs, reply *types.GetReply) {
	kv.mu.Lock()
	existing, ok := kv.KeyValueItems[args.Key]
	kv.mu.Unlock()

	if !ok {
		reply.Err = types.ErrNoKey
	} else {
		reply.Err = types.OK
		reply.Version = existing.Version
		reply.Value = existing.Value
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
//do we need to lock the state since we are pushing to a channel buffer
func (kv *KVServer) Put(args *types.PutArgs, reply *types.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	res := kv.handlePut(KeyValueItem{Key: args.Key, Value: args.Value, Version: args.Version})
	reply.Err = res
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer("1000")
	return []any{kv}
}

func (kv *KVServer) handlePut(kvi KeyValueItem) types.Err {
	existing, ok := kv.KeyValueItems[kvi.Key]
	if !ok {
		if kvi.Version != 0 {
			return types.ErrNoKey
		}
		kv.KeyValueItems[kvi.Key] = KeyValueItem{Key: kvi.Key, Value: kvi.Value, Version: 1}
		return types.OK
	}

	if existing.Version != kvi.Version {
		return types.ErrVersion
	}

	kv.KeyValueItems[kvi.Key] = KeyValueItem{Key: kvi.Key, Value: kvi.Value, Version: existing.Version + 1}

	return types.OK
}
