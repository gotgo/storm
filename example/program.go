package main

import "github.com/gotgo/storm"

func main() {
	runSpout()
}

func runSpout() {
	s := storm.NewStormSession()
	spout := storm.NewSpout(s, &MySpout{})
	go spout.Run()

	select {
	case <-s.Done:
	}

	close(s.Done)
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

type MySpout struct {
}

func (s *MySpout) Emit() *storm.TupleMessage {
	return nil
}
func (s *MySpout) Ack(id string) {

}
func (s *MySpout) Fail(id string) {

}
func (s *MySpout) AssociateTasks(id string, taskIds []int) {

}

type MyBolt struct {
}

func (mb *MyBolt) Process(tuple *storm.TupleMessage) (error, *storm.TupleMessage) {
	return nil, nil
}

func createTopology() {
	t := storm.NewTopology("word counter")
	parallelism := int32(2)

	randomScentence := t.AddSpout("randomScentence", &storm.ComponentDef{
		ShellCommand: "/opt/myapp/app",
		OutputFields: []string{},
		Direct:       false,
		Parallelism:  parallelism,
	})

	splitter := t.AddBolt("wordSplitter", &storm.ComponentDef{
		ShellCommand: "/opt/mybolt/app",
		OutputFields: []string{},
		Direct:       false,
		Parallelism:  parallelism,
	})
	splitter.Input(randomScentence, storm.DistributeByShuffle, nil)

	wordCounter := t.AddBolt("wordCounter", &storm.ComponentDef{
		ShellCommand: "/opt/mybolt/app",
		OutputFields: []string{},
		Direct:       false,
		Parallelism:  parallelism,
	})
	wordCounter.Input(splitter, storm.DistributeByShuffle, nil)
}
