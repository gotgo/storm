package storm

import (
	"encoding/json"
	"time"
)

// NewSpout - Creates a new spout for the given storm session.
func NewSpout(s *Storm) *Spout {
	return &Spout{
		storm: s,
	}
}

// Spout - Emits data
// Input: (Message)
type Spout struct {
	storm *Storm
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
	//emit

	//TODO: then get TaskIds as json array
	bts := <-s.storm.Input
	taskIds := make(TaskIds, 0)
	json.Unmarshal(bts, &taskIds)
	return false
}

// emitDirect - to a specific task number
func (s *Spout) emitDirect() bool {
	//emit

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
}

func (s *Spout) fail(id string) {
	//record as fail, to retry
}
