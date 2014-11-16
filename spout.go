package storm

import (
	"encoding/json"
	"time"
)

// NewSpout - Creates a new spout for the given storm session.
func NewSpout(s *Storm, spout Spouter) *Spout {
	return &Spout{
		storm:   s,
		spouter: spout,
	}
}

// Spout - Emits data
// Input: (Message)
type Spout struct {
	storm   *Storm
	spouter Spouter
}

// Run - Runs the spout
func (s *Spout) Run() {
	for {
		select {
		case bts := <-s.storm.Input:
			msg := new(SpoutMessage)
			if err := json.Unmarshal(bts, &msg); err != nil {
				panic(err)
			}
			switch msg.Command {
			case "next":
				s.next()
			case "ack":
				s.ack(msg.Id)
			case "fail":
				s.fail(msg.Id)
			}
			s.sync()
		case <-s.storm.Done:
			return
		}
	}
}

func (s *Spout) emit() bool {
	tuple := s.spouter.Emit()
	if tuple == nil {
		return false
	}

	msg := &SpoutMessage{TupleMessage: *tuple, Command: "emit"}
	s.storm.Output <- msg

	bts := <-s.storm.Input
	taskIds := make(TaskIds, 0)
	err := json.Unmarshal(bts, &taskIds)
	if err != nil {
		panic(err)
	}
	s.spouter.AssociateTasks(tuple.Id, taskIds)
	return true
}

// emitDirect - to a specific task number
func (s *Spout) emitDirect() bool {
	//emit
	panic("todo")
	//no task ids
	return false
}

func (s *Spout) sync() {
	s.storm.Output <- &SpoutMessage{
		Command: "sync",
	}
}

func (s *Spout) next() {
	if !s.emit() {
		select {
		case <-time.After(time.Millisecond * 100):
		case <-s.storm.Done:
			return
		}
	}
}

func (s *Spout) ack(id string) {
	//record as complete
	s.spouter.Ack(id)
}

func (s *Spout) fail(id string) {
	//record as fail, to retry
	s.spouter.Fail(id)
}
