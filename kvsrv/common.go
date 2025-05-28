package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key         string
	Value       string
	ClientID    int64
	OperationID int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	ClientID int64
	OperationID int64
}

type GetReply struct {
	Value string
}
