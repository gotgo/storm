package storm

import (
	"encoding/json"
	"fmt"
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

	if tuple.Task == nil {
		waitFor := time.Second * 5
		done := false
		//this ensures that if for some reason we never get input that we are not stuck and can at least
		//gracefully shutdown
		for {
			select {
			case bts := <-s.storm.Input:
				taskIds := make(TaskIds, 0)
				err := json.Unmarshal(bts, &taskIds)
				if err != nil {
					panic(err)
				}
				s.spouter.AssociateTasks(tuple.Id, taskIds)
				break
			case <-s.storm.Done:
				done = true
				continue
			case <-time.After(waitFor):
				if done {
					break
				} else {
					s.storm.Log(fmt.Sprintf("Warning: spout waiting for TaskIds for tupleId:'%s'", tuple.Id))
				}
			}
		}
	}
	return true
}

func (s *Spout) sync() {
	s.storm.Output <- &SpoutMessage{
		Command: "sync",
	}
}

func (s *Spout) next() {
	if s.emit() == false {
		select {
		case <-time.After(time.Millisecond * 100):
		case <-s.storm.Done:
			return
		}
	}
}

func (s *Spout) ack(id string) {
	s.spouter.Ack(id)
}

func (s *Spout) fail(id string) {
	s.spouter.Fail(id)
}
