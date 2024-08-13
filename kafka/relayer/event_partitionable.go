package relayer

type EventPartitionable interface {

	// Partition specify partition for relayed message.
	Partition() int32
}
