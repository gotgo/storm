package storm_test

import (
	"encoding/json"

	. "github.com/gotgo/storm"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func NewTestSpout() *TestSpout {
	return &TestSpout{
		Data:        make([]*TupleMessage, 0),
		Acks:        make(map[string]int),
		Fails:       make(map[string]int),
		LastTaskIds: make(map[string][]int),
	}
}

type TestSpout struct {
	Data        []*TupleMessage
	Acks        map[string]int
	Fails       map[string]int
	LastTaskIds map[string][]int
}

func (s *TestSpout) Emit() *TupleMessage {
	if len(s.Data) > 0 {
		n := s.Data[0]
		s.Data = s.Data[1:]
		return n
	} else {
		return nil
	}
}
func (s *TestSpout) Ack(id string) {
	s.Acks[id] += 1
}
func (s *TestSpout) Fail(id string) {
	s.Fails[id] += 1
}
func (s *TestSpout) AssociateTasks(id string, taskIds []int) {
	s.LastTaskIds[id] = taskIds
}

func marshal(o interface{}) []byte {
	if b, e := json.Marshal(o); e != nil {
		panic(e)
	} else {
		return b
	}
}

var _ = Describe("Spout", func() {

	var testSpout *TestSpout
	var spout *Spout
	var channel *Storm

	BeforeEach(func() {
		testSpout = NewTestSpout()
		channel = &Storm{
			Input:  make(chan []byte),
			Output: make(chan interface{}),
			Done:   make(chan struct{}),
		}
		spout = NewSpout(channel, testSpout)

		go spout.Run()
	})

	AfterEach(func() {
		close(channel.Done)
	})

	It("should ack", func() {
		m := &SpoutMessage{
			TupleMessage: TupleMessage{Id: "fred"},
			Command:      "ack",
		}
		channel.Input <- marshal(m)
		Expect(testSpout.Acks[m.Id]).To(Equal(1))

		o := <-channel.Output
		msgOut := o.(*SpoutMessage)
		Expect(msgOut.Command).To(Equal("sync"))
	})

	It("should fail", func() {
		m := &SpoutMessage{
			TupleMessage: TupleMessage{Id: "sam"},
			Command:      "fail",
		}
		channel.Input <- marshal(m)

		o := <-channel.Output
		msgOut := o.(*SpoutMessage)
		Expect(msgOut.Command).To(Equal("sync"))
		Expect(testSpout.Fails[m.Id]).To(Equal(1))
	})

	It("should emit data", func() {
		d := make([]interface{}, 2)
		d[0] = "hot"
		d[1] = 7

		t := &TupleMessage{
			Id:    "12345",
			Tuple: d,
		}
		testSpout.Data = append(testSpout.Data, t)

		channel.Input <- marshal(&SpoutMessage{Command: "next"})

		o := <-channel.Output
		msgOut := o.(*SpoutMessage)

		Expect(msgOut.Command).To(Equal("emit"))
		Expect(msgOut.Tuple[0]).To(Equal(d[0]))
		Expect(msgOut.Tuple[1]).To(Equal(d[1]))

		taskIds := []int{2}
		channel.Input <- marshal(taskIds)

		o = <-channel.Output
		msgOut = o.(*SpoutMessage)
		Expect(msgOut.Command).To(Equal("sync"))

		//needs to go after the channel on output otherwise it's a race condition
		ids := testSpout.LastTaskIds[t.Id]
		Expect(ids).ToNot(BeNil())
		Expect(ids[0]).To(Equal(taskIds[0]))
	})

	It("should emit safely with no data", func() {

	})

})
