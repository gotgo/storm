package storm

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/amattn/deeperror"
)

// Storm - The storm Processor for running a Spout or a Bolt
type Storm struct {
	Input  chan []byte
	Output chan interface{}
	done   chan struct{}
	ExtIn  io.Reader
	ExtOut io.Writer
}

// NewStormSession - Connects with Storm and starts the processor for running a Bolt or Spout
func NewStorm() *Storm {
	return &Storm{
		Input:  make(chan []byte),
		Output: make(chan interface{}),
		done:   make(chan struct{}),
		ExtIn:  os.Stdin,
		ExtOut: os.Stdout,
	}
}

func (s *Storm) Run() {
	go s.read()
	go s.write()
	s.connect()
}

func (s *Storm) End() {
	close(s.done)
}

func (s *Storm) connect() {
	setup := new(ConnectInfo)
	if err := json.Unmarshal(<-s.Input, &setup); err != nil {
		panic(deeperror.New(rand.Int63(), "connect unmarshal fail", err))
	}
	pid := strconv.Itoa(os.Getpid())
	file, err := os.Create(path.Join(setup.PidDir, pid))
	if err != nil {
		panic(err)
	}
	file.Close()
	s.Output <- &ProcessId{pid}
}

func (s *Storm) read() {
	bs := bytes.NewBufferString("")
	r := bufio.NewReader(s.ExtIn)
	for {
		if line, err := r.ReadString('\n'); err == io.EOF {
			time.Sleep(time.Millisecond * 300)
		} else {
			if line == "end" {
				s.Input <- bs.Bytes()
				bs.Truncate(0)
			} else {
				bs.WriteString(line)
			}
		}
		select {
		case <-s.done:
			return
		default:
		}
	}
}

func (s *Storm) write() {
	w := bufio.NewWriter(s.ExtOut)
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
		case <-s.done:
			return
		}
	}
}
