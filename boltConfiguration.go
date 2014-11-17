package storm

import "github.com/gotgo/storm/thrift/topology"

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
		ComponentId: source.Name(),
		StreamId:    "default",
	}

	bc.bolt.Common.Inputs[streamId] = g
}
