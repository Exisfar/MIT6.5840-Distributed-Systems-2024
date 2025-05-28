package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu     sync.Mutex
	data   map[string]string // key-value store
	lastOp map[int64]struct {
		opID  int64
		value string
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查是否是重复请求
	if last, exists := kv.lastOp[args.ClientID]; exists && args.OperationID <= last.opID {
		reply.Value = last.value
		return
	}

	// 新请求处理
	oldValue := kv.data[args.Key]
	kv.data[args.Key] = args.Value

	// 更新最后操作记录
	kv.lastOp[args.ClientID] = struct {
		opID  int64
		value string
	}{args.OperationID, oldValue}

	reply.Value = oldValue
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查是否是重复请求
	if last, exists := kv.lastOp[args.ClientID]; exists && args.OperationID <= last.opID {
		reply.Value = last.value
		return
	}

	// 新请求处理
	oldValue := kv.data[args.Key]
	kv.data[args.Key] += args.Value

	// 更新最后操作记录
	kv.lastOp[args.ClientID] = struct {
		opID  int64
		value string
	}{args.OperationID, oldValue}

	reply.Value = oldValue
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		data: make(map[string]string),
		lastOp: make(map[int64]struct {
			opID  int64
			value string
		}),
	}
	return kv
}