package storm_test

import (
	"encoding/json"

	. "github.com/gotgo/storm"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func NewTestSpout() *TestSpout {
	return &TestSpout{
		Data:        make(chan *TupleMessage),
		Acks:        make(map[string]int),
		Fails:       make(map[string]int),
		LastTaskIds: make(map[string]TaskIds),
	}
}

type TestSpout struct {
	Data        chan *TupleMessage
	Acks        map[string]int
	Fails       map[string]int
	LastTaskIds map[string]TaskIds
}

func (s *TestSpout) Emit() *TupleMessage {
	return <-s.Data
}
func (s *TestSpout) Ack(id string) {
	s.Acks[id] += 1
}
func (s *TestSpout) Fail(id string) {
	s.Fails[id] += 1
}
func (s *TestSpout) AssociateTasks(id string, taskIds []byte) {
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
		testSpout = &TestSpout{}
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

		o := <-channel.Output
		msgOut := o.(*SpoutMessage)
		Expect(msgOut.Command).To(Equal("ack"))
		Expect(testSpout.Acks[m.Id]).To(Equal(1))

		o = <-channel.Output
		msgOut = o.(*SpoutMessage)
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
		Expect(msgOut.Command).To(Equal("fail"))
		Expect(testSpout.Fails[m.Id]).To(Equal(1))

		o = <-channel.Output
		msgOut = o.(*SpoutMessage)
		Expect(msgOut.Command).To(Equal("sync"))
	})

	It("should emit", func() {
		d := make([]interface{}, 2)
		d[0] = "hot"
		d[1] = 7
		testSpout.Data <- &TupleMessage{
			Id:    "12345",
			Tuple: d,
		}

		m := &SpoutMessage{
			TupleMessage: TupleMessage{Id: "mike"},
			Command:      "next",
		}
		channel.Input <- marshal(m)

		o := <-channel.Output
		msgOut := o.(*SpoutMessage)
		Expect(msgOut.Command).To(Equal("emit"))
		Expect(msgOut.Tuple[0]).To(Equal(d[0]))
		Expect(msgOut.Tuple[1]).To(Equal(d[1]))

		taskIds := []int{2}
		channel.Input <- marshal(taskIds)

		Expect(testSpout.LastTaskIds[m.Id][0]).To(Equal(taskIds[0]))

		o = <-channel.Output
		msgOut = o.(*SpoutMessage)
		Expect(msgOut.Command).To(Equal("sync"))
	})

})
