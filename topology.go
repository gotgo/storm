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

type ComponentDef struct {
	ShellCommand string //to execute
	OutputFields []string
	Direct       bool
	Parallelism  int32
}

func (t *Topology) AddSpout(name string, spout *ComponentDef) EmiterName {
	if spout.Parallelism <= 0 {
		spout.Parallelism = 1
	}

	s := &topology.SpoutSpec{
		SpoutObject: &topology.ComponentObject{
			Shell: &topology.ShellComponent{
				ExecutionCommand: spout.ShellCommand,
				Script:           "",
			},
		},
		Common: &topology.ComponentCommon{
			Inputs:          make(map[*topology.GlobalStreamId]*topology.Grouping),
			Streams:         make(map[string]*topology.StreamInfo),
			ParallelismHint: thrift.Int32Ptr(spout.Parallelism),
			JsonConf:        thrift.StringPtr(""),
		},
	}

	s.Common.Streams["default"] = &topology.StreamInfo{
		OutputFields: spout.OutputFields,
		Direct:       spout.Direct,
	}
	t.topo.Spouts[name] = s
	return &emiterName{name}
}

func (t *Topology) AddBolt(name string, bolt *ComponentDef) *BoltConfiguration {
	b := &topology.Bolt{
		BoltObject: &topology.ComponentObject{
			Shell: &topology.ShellComponent{
				ExecutionCommand: bolt.ShellCommand,
				Script:           "",
			},
		},
		Common: &topology.ComponentCommon{
			Inputs:          make(map[*topology.GlobalStreamId]*topology.Grouping),
			Streams:         make(map[string]*topology.StreamInfo),
			ParallelismHint: thrift.Int32Ptr(bolt.Parallelism),
			JsonConf:        thrift.StringPtr(""),
		},
	}

	b.Common.Streams["default"] = &topology.StreamInfo{
		OutputFields: bolt.OutputFields,
		Direct:       bolt.Direct,
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
