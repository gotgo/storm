package storm

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gotgo/fw/me"
	"github.com/gotgo/fw/stats"
)

// NewSpout - Creates a new spout for the given storm session.
func NewSpout(s *Storm, spout Spouter) *Spout {
	return &Spout{
		storm:   s,
		spouter: spout,
		track:   stats.NewBasicMeter("spout", me.App.Environment()),
	}
}

// Spout - Emits data
// Input: (Message)
type Spout struct {
	storm   *Storm
	spouter Spouter
	track   stats.BasicMeter
}

func mustUnmarshal(b []byte, i interface{}) {
	if err := json.Unmarshal(b, &i); err != nil {
		panic(err)
	}
}

// Run - Runs the spout
func (s *Spout) Run() {
	for {
		select {
		case bts := <-s.storm.Input:
			if bts[0] == '[' {
				var ids []int
				mustUnmarshal(bts, &ids)
			} else {
				msg := new(SpoutMessage)
				mustUnmarshal(bts, &msg)
				switch msg.Command {
				case "next":
					s.next()
				case "ack":
					s.ack(msg.Id)
				case "fail":
					s.fail(msg.Id)
				default:
					panic("unknown command " + msg.Command)
				}
				s.sync()
			}
		case <-s.storm.Done:
			return
		}
	}
}

func (s *Spout) emit() bool {
	if tuple := s.spouter.Emit(); tuple == nil {
		return false
	} else {
		s.track.Occurence("emit")
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
					taskIds := make([]int, 0)
					mustUnmarshal(bts, &taskIds)
					s.spouter.AssociateTasks(tuple.Id, taskIds)
					return true
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
			return
		case <-s.storm.Done:
			return
		}
	}
}

func (s *Spout) ack(id string) {
	s.track.Occurence("ack")
	s.spouter.Ack(id)
}

func (s *Spout) fail(id string) {
	s.track.Occurence("fail")
	s.spouter.Fail(id)
}
