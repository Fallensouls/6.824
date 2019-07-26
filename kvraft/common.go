package raftkv

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrExecuted    = "ErrExecuted"
	ErrTimeout     = "ErrTimeout"
	ErrPartitioned = "ErrPartitioned"
	ErrShutdown    = "ErrShutdown"
)

type Err string

// Put or Append
type PutAppendRequest struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID  string // id of the client
	Seq uint64 // unique serial number to this command
}

type PutAppendResponse struct {
	WrongLeader bool
	Err         Err
}

type GetRequest struct {
	Key string
	// You'll have to add definitions here.
	ID string // id of the client
}

type GetResponse struct {
	WrongLeader bool
	Err         Err
	Value       string
}
