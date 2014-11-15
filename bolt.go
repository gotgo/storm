package storm

import (
	"encoding/json"
	"fmt"
)

// NewBold - Creates a new Bolt for transformations
func NewBolt(s *Storm, t Transformer) *Bolt {
	return &Bolt{
		storm:       s,
		transformer: t,
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
	storm       *Storm
	transformer Transformer
}

// Process - Process all tuples that come into the bold
func (b *Bolt) Process() {
	for {
		select {
		case bts := <-b.storm.Input:
			if bts[0] == '[' {
				var ids TaskIds
				if err := json.Unmarshal(bts, &ids); err != nil {
					panic(err)
				}
				b.receiveTaskIds(ids)
			} else {
				var input BoltInput
				if err := json.Unmarshal(bts, &input); err != nil {
					panic(err)
				}
				b.transform(&input)
			}
			break //TaskIds or Message
		case <-b.storm.Done:
			return
		}
	}
}

func (b *Bolt) receiveTaskIds(taskIds []int) {
	//not sure what to do with these
}

func (b *Bolt) transform(bi *BoltInput) {
	err, newTuple := b.transformer.Transform(&bi.TupleMessage)

	if err != nil {
		b.storm.Log(fmt.Sprintf("Fail id:{0} error:{1}", bi.TupleMessage.Id, err.Error()))
		b.fail(bi.Id)
	} else if newTuple == nil {
		b.ack(bi.Id)
	} else {
		b.emit(newTuple)
	}
}

func (b *Bolt) emit(tm *TupleMessage) {
	output := &BoltOutput{
		TupleMessage: *tm,
		Command:      "emit",
	}
	b.storm.Output <- output
}

func (b *Bolt) fail(id string) {
	output := &BoltOutput{
		Command:      "fail",
		TupleMessage: TupleMessage{Id: id},
	}

	b.storm.Output <- output
}

func (b *Bolt) ack(id string) {
	output := &BoltOutput{
		Command:      "ack",
		TupleMessage: TupleMessage{Id: id},
	}
	b.storm.Output <- output
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
