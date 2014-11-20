package storm

import (
	"encoding/json"
	"fmt"
)

// NewBold - Creates a new Bolt for transformations
func NewBolt(s *Storm, p BoltProcessor) *Bolt {
	return &Bolt{
		storm:     s,
		processor: p,
	}
}

// Bolt - Applies a transformation on an input tuple
// Input Types (2 different):
// * Tuple (BoltInput)
// * TaskIds (from a previous emit)
//
// Output: (BoltOutput)
// * Tupel (now changed in some way)
// * Command - ack, fail, log, emit
type Bolt struct {
	storm     *Storm
	processor BoltProcessor
}

// Process - Process all tuples that come into the bold
func (b *Bolt) Run() {
	for {
		select {
		case bts := <-b.storm.Input:
			if bts[0] == '[' {
				var ids []int
				if err := json.Unmarshal(bts, &ids); err != nil {
					panic(err)
				}
				b.trackIndirectEmit(ids)
			} else {
				var input BoltInput
				if err := json.Unmarshal(bts, &input); err != nil {
					panic(err)
				}
				b.transform(&input)
			}
			break //TaskIds or Message
		case <-b.storm.done:
			return
		}
	}
}

func (b *Bolt) trackIndirectEmit(taskIds []int) {
	b.processor.TrackIndirectEmit(taskIds)
}

func (b *Bolt) transform(bi *BoltInput) {
	newTuple, err := b.processor.Process(&bi.TupleMessage)

	if err != nil {
		b.log(fmt.Sprintf("Fail id:{0} error:{1}", bi.TupleMessage.Id, err.Error()))
		b.fail(bi.Id)
	} else if newTuple == nil {
		b.ack(bi.Id)
	} else {
		b.emit(newTuple)
		b.ack(bi.Id)
	}
}

func (s *Bolt) log(message string) {
	s.storm.Output <- &BoltOutput{
		Command: "log",
		Message: message,
	}
}

func (b *Bolt) emit(tm *TupleMessage) {
	b.storm.Output <- &BoltOutput{
		TupleMessage: *tm,
		Command:      "emit",
	}
}

func (b *Bolt) fail(id string) {
	b.storm.Output <- &BoltOutput{
		Command:      "fail",
		TupleMessage: TupleMessage{Id: id},
	}
}

func (b *Bolt) ack(id string) {
	b.storm.Output <- &BoltOutput{
		Command:      "ack",
		TupleMessage: TupleMessage{Id: id},
	}
}

//BoltInput - Inbound Tuple
type BoltInput struct {
	//TupleMessage - the tuple and it's metadata
	TupleMessage

	//Component - The id of the creating component
	Component string `json:"comp"`
}

//BoltInput - Outbound Tuple
type BoltOutput struct {

	//TupleMessage - the tuple and it's metadata
	TupleMessage

	//Anchors - The ids of the tuples these output tuples should be anchored to
	Anchors []string `json:"anchors"`

	//Command - ack, fail, emit, log
	Command string `json:"command"`

	//Message - for log only
	Message string `json:"message",omitempty`
}
