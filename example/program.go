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

func runBolt() {
	storm := storm.NewStormSession()
	b := storm.NewBolt(storm)
	go b.Process()

	select {
	case <-storm.Done:
	}

	close(storm.Done)
}
