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
type TupleProcessor interface {
	Process(tuple *TupleMessage) (error, *TupleMessage)
}

type Spouter interface {
	Emit() *TupleMessage
	Ack(id string)
	Fail(id string)
	AssociateTasks(id string, taskIds []int)
}

type SpoutProcess struct {
	ExecutionCommand string
}

type BoltProcess struct {
	ExecutionCommand string
}
