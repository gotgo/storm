package storm

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strconv"
)

// Storm - The storm Processor for running a Spout or a Bolt
type Storm struct {
	Input  chan []byte
	Output chan interface{}
	Done   chan struct{}
}

// NewStormSession - Connects with Storm and starts the processor for running a Bolt or Spout
func NewStormSession() *Storm {
	s := &Storm{
		Input:  make(chan []byte),
		Output: make(chan interface{}),
		Done:   make(chan struct{}),
	}
	go s.read()
	go s.write()
	s.connect()
	return s
}

// Log - Send a log message to Storm
func (s *Storm) Log(message string) {
	s.Output <- &SpoutMessage{
		Command: "log",
		Message: message,
	}
}

func (s *Storm) connect() {
	setup := new(setupInfo)
	if err := json.Unmarshal(<-s.Input, &setup); err != nil {
		panic(err)
	}
	pid := strconv.Itoa(os.Getpid())
	file, err := os.Create(path.Join(setup.PidDir, pid))
	if err != nil {
		panic(err)
	}
	file.Close()
	s.Output <- &processId{pid}
}

type processId struct {
	Pid string `json:"pid"`
}

type setupInfo struct {
	Conf    *json.RawMessage `json:"conf"`
	Context *json.RawMessage `json:"context"`
	PidDir  string           `json:"pidDir"`
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
