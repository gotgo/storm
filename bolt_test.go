package storm_test

import (
	"encoding/json"
	"errors"

	. "github.com/gotgo/storm"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bolt", func() {

	var testBolt *TestBolt
	var bolt *Bolt
	var channel *Storm

	BeforeEach(func() {
		testBolt = &TestBolt{}
		channel = NewStorm()
		bolt = NewBolt(channel, testBolt)
		go bolt.Run()
	})

	AfterEach(func() {
		channel.End()
	})

	assertShouldFail := func() {
		inMsg := &BoltInput{TupleMessage: TupleMessage{Id: "1515"}}
		bts, _ := json.Marshal(inMsg)
		channel.Input <- bts

		o := <-channel.Output
		outMsg := o.(*BoltOutput)
		ExpectWithOffset(1, outMsg.Command).To(Equal("log"))

		o = <-channel.Output
		outMsg = o.(*BoltOutput)
		ExpectWithOffset(1, outMsg.Command).To(Equal("fail"))
		ExpectWithOffset(1, outMsg.Id).To(Equal(inMsg.Id))
	}

	It("should fail on error", func() {
		testBolt.Handler = func(t *TupleMessage) (*TupleMessage, error) {
			return nil, errors.New("fail on purpose")
		}
		assertShouldFail()
	})
	It("should fail on error even if there is an output tuple", func() {
		testBolt.Handler = func(t *TupleMessage) (*TupleMessage, error) {
			return &TupleMessage{Id: "ha"}, errors.New("fail on purpose")
		}
		assertShouldFail()
	})

	It("should ack when no output tuple", func() {
		testBolt.Handler = func(t *TupleMessage) (*TupleMessage, error) {
			return nil, nil
		}
		inMsg := &BoltInput{TupleMessage: TupleMessage{Id: "1566"}}
		bts, _ := json.Marshal(inMsg)
		channel.Input <- bts

		o := <-channel.Output
		outMsg := o.(*BoltOutput)
		Expect(outMsg.Command).To(Equal("ack"))
		Expect(outMsg.Id).To(Equal(inMsg.Id))
	})

	It("should emit and ack", func() {
		tupOut := &TupleMessage{
			Id: "out",
		}
		testBolt.Handler = func(t *TupleMessage) (*TupleMessage, error) {
			return tupOut, nil
		}
		inMsg := &BoltInput{TupleMessage: TupleMessage{Id: "9999"}}
		bts, _ := json.Marshal(inMsg)
		channel.Input <- bts

		o := <-channel.Output
		outMsg := o.(*BoltOutput)
		Expect(outMsg.Command).To(Equal("emit"))
		Expect(outMsg.Id).To(Equal(tupOut.Id))

		o = <-channel.Output
		outMsg = o.(*BoltOutput)

		Expect(outMsg.Command).To(Equal("ack"))
		Expect(outMsg.Id).To(Equal(inMsg.Id))
	})

	It("should receive task ids", func() {
		ids := []int{3, 2}
		bts, _ := json.Marshal(ids)
		channel.Input <- bts
		Expect(testBolt.TaskIds).ToNot(BeNil())
		Expect(testBolt.TaskIds).To(Equal(ids))
	})
})

type TestBolt struct {
	Handler func(tuple *TupleMessage) (*TupleMessage, error)
	TaskIds []int
}

func (b *TestBolt) Process(tuple *TupleMessage) (*TupleMessage, error) {
	if b.Handler == nil {
		return nil, nil
	}
	return b.Handler(tuple)
}

func (b *TestBolt) TrackIndirectEmit(taskIds []int) {
	b.TaskIds = taskIds
}
