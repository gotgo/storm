package storm

//TupleMessage - A tuple an it's processing metadata
type TupleMessage struct {

	//Id - To identifiy this tuple, for messaging guarantees
	Id string `json:"id",omitempty`

	//Stream - Id of the stream this tuple is emmited to. Blank==default stream
	Stream string `json:"stream",omitempty`

	//Task - For direct emit
	Task *int32 `json:"task",omitempty`

	//Tuple - A 'row' of data
	Tuple []interface{} `json:"tuple"`
}

//Spout Input & Output
type SpoutMessage struct {

	//TupleMessage - the tuple and it's metadata
	TupleMessage

	// Command - next, ack, fail, emit, log, sync
	Command string `json:"command"`

	//Message - for log only
	Message string `json:"message",omitempty`
}

// Transformer - Enables an implementation of a Bolt Transform
type BoltProcessor interface {
	Process(tuple *TupleMessage) (*TupleMessage, error)
	TrackIndirectEmit(taskIds []int)
}

type Spouter interface {
	Emit() *TupleMessage
	Ack(id string)
	Fail(id string)
	AssociateTasks(id string, taskIds []int)
}

type ComponentDef struct {
	ShellCommand string //to execute
	OutputFields []string
	Direct       bool
	Parallelism  int32
}

type DistributeHow string

const (
	DistributeByShuffle      = "shuffle"
	DistributeByField        = "field"
	DistributeToAll          = "all"
	DistributeDirect         = "direct"
	DistributeAny            = "none" //none means no choice, don't care
	DistributeLocalOrShuffle = "localorshuffle"
)
