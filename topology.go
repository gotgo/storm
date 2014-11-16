package storm

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/gotgo/storm/thrift/topology"
)

func NewTopology(name string) *Topology {
	t := topology.NewStormTopology()
	t.Spouts = make(map[string]*topology.SpoutSpec)
	t.Bolts = make(map[string]*topology.Bolt)
	t.StateSpouts = make(map[string]*topology.StateSpoutSpec)
	return &Topology{
		topo: t,
	}
}

type Topology struct {
	topo *topology.StormTopology
}

type EmiterName interface {
	Name() string
}

type emiterName struct {
	name string
}

func (n emiterName) Name() string {
	return n.name
}

func (t *Topology) AddSpout(name string, spout *SpoutProcess, parallelism int32) EmiterName {
	s := &topology.SpoutSpec{
		SpoutObject: &topology.ComponentObject{
			Shell: &topology.ShellComponent{
				ExecutionCommand: spout.ExecutionCommand,
				Script:           "",
			},
		},
		Common: &topology.ComponentCommon{
			Inputs:          make(map[*topology.GlobalStreamId]*topology.Grouping),
			Streams:         make(map[string]*topology.StreamInfo),
			ParallelismHint: thrift.Int32Ptr(parallelism),
			JsonConf:        thrift.StringPtr(""),
		},
	}
	t.topo.Spouts[name] = s
	return &emiterName{name}
}

func (t *Topology) AddBolt(name string, bolt *BoltProcess, parallelism int32) *BoltConfiguration {
	b := &topology.Bolt{
		BoltObject: &topology.ComponentObject{
			Shell: &topology.ShellComponent{
				ExecutionCommand: bolt.ExecutionCommand,
				Script:           "",
			},
		},
		Common: &topology.ComponentCommon{
			Inputs:          make(map[*topology.GlobalStreamId]*topology.Grouping),
			Streams:         make(map[string]*topology.StreamInfo),
			ParallelismHint: thrift.Int32Ptr(parallelism),
			JsonConf:        thrift.StringPtr(""),
		},
	}
	t.topo.Bolts[name] = b
	return &BoltConfiguration{
		bolt: b,
		name: name,
	}
}

type DistributeHow string

const (
	DistributeByShuffle      = "shuffle"
	DistributeByField        = "field"
	DistributeToAll          = "all"
	DistributeDirect         = "direct"
	DistributeAny            = "none" //none means no choice, don't care
	DistributeLocalOrShuffle = "localorshuffle"
)

type BoltConfiguration struct {
	bolt *topology.Bolt
	name string
}

//Emiter interface implementation
func (bc *BoltConfiguration) Name() string {
	return bc.name
}

func (bc *BoltConfiguration) Input(source EmiterName, distributeHow DistributeHow, fields []string) {
	g := &topology.Grouping{
		Fields:         nil,
		Shuffle:        nil,
		All:            nil,
		None:           nil,
		Direct:         nil,
		LocalOrShuffle: nil,
	}

	yes := &topology.NullStruct{}

	switch distributeHow {
	case DistributeByShuffle:
		g.Shuffle = yes
	case DistributeByField:
		g.Fields = fields
	case DistributeToAll:
		g.All = yes
	case DistributeDirect:
		g.Direct = yes
	case DistributeAny:
		g.None = yes
	case DistributeLocalOrShuffle:
		g.LocalOrShuffle = yes
	}

	streamId := &topology.GlobalStreamId{
		ComponentId: source.Name(), //the bolt or spout name
		StreamId:    "default",
	}

	bc.bolt.Common.Inputs[streamId] = g

}
