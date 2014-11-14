package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"
)

func main() {

	storm := NewStormSession()
	storm.Connect()
	go storm.Run()

	select {
	case <-storm.Done:
	}

	close(storm.Done)
}

type TaskIds []int

func (s *Storm) Run() {
	for {
		select {
		case bts := <-s.Input:
			msg := new(Message)
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
		case <-s.Done:
			return
		}
	}
}

func (s *Storm) sync() {
	s.Output <- &Message{
		Command: "sync",
	}
}

func (s *Storm) log(message string) {
	s.Output <- &Message{
		Message: message,
	}
}

func (s *Storm) emit() bool {
	//emit

	//TODO: then get TaskIds as json array
	bts := <-s.Input
	taskIds := make(TaskIds, 0)
	json.Unmarshal(bts, &taskIds)
	return false
}

// emitDirect - to a specific task number
func (s *Storm) emitDirect() bool {
	//emit

	//no task ids
	return false
}

func (s *Storm) next() {
	if !s.emit() {
		select {
		case <-time.After(time.Millisecond * 100):
		case <-s.Done:
			return
		}
	}
}

func (s *Storm) ack(id string) {
	//record as complete
}

func (s *Storm) fail(id string) {
	//record as fail, to retry
}

func (s *Storm) Connect() {
	setup := new(SetupInfo)
	if err := json.Unmarshal(<-s.Input, &setup); err != nil {
		panic(err)
	}
	pid := strconv.Itoa(os.Getpid())
	file, err := os.Create(path.Join(setup.PidDir, pid))
	if err != nil {
		panic(err)
	}
	file.Close()
	s.Output <- &Pid{pid}
}

type Pid struct {
	Pid string `json:"pid"`
}

type SetupInfo struct {
	Conf    *json.RawMessage `json:"conf"`
	Context *json.RawMessage `json:"context"`
	PidDir  string           `json:"pidDir"`
}

type Message struct {
	// Command - next, ack, fail, emit, log, sync
	Command string        `json:"command"`
	Id      string        `json:"id", omitempty`
	Stream  string        `json:"stream",omitempty`
	Task    int           `json:"task", omitempty`
	Tuple   []interface{} `json:"tuple", omitempty`
	Message string        `json:"msg", omitempty`
}

type Storm struct {
	Input  chan []byte
	Output chan interface{}
	Done   chan struct{}
}

func NewStormSession() *Storm {
	s := &Storm{
		Input:  make(chan []byte),
		Output: make(chan interface{}),
		Done:   make(chan struct{}),
	}
	go s.read()
	go s.write()
	return s
}

func (s *Storm) read() {
	r := bufio.NewReader(os.Stdin)
	bs := bytes.NewBufferString("")
	for {
		if line, err := r.ReadString('\n'); err != nil {
			panic(err)
		} else {
			if line == "end" {
				s.Input <- bs.Bytes()
				bs.Truncate(0)
			} else {
				bs.WriteString(line)
			}
		}

		select {
		case <-s.Done:
			return
		default:
		}
	}
}

func (s *Storm) write() {
	w := bufio.NewWriter(os.Stdout)
	for {
		select {
		case obj := <-s.Output:
			if bts, err := json.Marshal(obj); err != nil {
				panic(err)
			} else {
				w.Write(bts)
				fmt.Fprintln(w)
				if _, err := fmt.Fprintln(w, "end"); err != nil {
					panic(err)
				}
			}

			w.Flush()
		case <-s.Done:
			return
		}
	}
}
