package storm

// TaskIds - an array of ids
type TaskIds []int

//TupleMessage - A tuple an it's processing metadata
type TupleMessage struct {

	//Id - for ack & fail only
	Id string `json:"id",omitempty`

	//Stream - Id of the stream this tuple is emmited to. Blank==default stream
	Stream string `json:"stream",omitempty`

	//Task - For direct emit
	Task int `json:"task",omitempty`

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
type Transformer interface {
	Transform(tuple *TupleMessage) (error, *TupleMessage)
}
