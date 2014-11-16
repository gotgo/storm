package main

import "github.com/gotgo/storm"

func main() {
	runSpout()
}

func runSpout() {
	storm := storm.NewStormSession()
	spout := storm.NewSpout(storm)
	go spout.Run()

	select {
	case <-storm.Done:
	}

	close(storm.Done)
}

type MyBolt struct {
}

func (mb *MyBolt) Transform(tuple *storm.TupleMessage) (error, *storm.TupleMessage) {
	return nil, nil
}

func runBolt() {
	s := storm.NewStormSession()
	b := storm.NewBolt(s, &MyBolt{})
	go b.Process()

	select {
	case <-s.Done:
	}

	close(s.Done)
}

func createTopology() {
	t := storm.NewTopology("word counter")
	parallelism := int32(2)

	randomScentence := t.AddSpout("randomScentence", &storm.SpoutProcess{"/opt/myapp/app"}, parallelism)

	splitter := t.AddBolt("wordSplitter", &storm.BoltProcess{"/opt/mybolt/app"}, parallelism)
	splitter.Input(randomScentence, storm.DistributeByShuffle, nil)

	wordCounter := t.AddBolt("wordCounter", &storm.BoltProcess{"/opt/mybolt/app"}, parallelism)
	wordCounter.Input(splitter, storm.DistributeByShuffle, nil)
}
