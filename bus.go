package mess

type Event struct {
	// ID of the event. Unique among events published by this node. Set by the node.
	ID uint64
	// NodeID is the ID of the node that published the event. Set by the node.
	NodeID uint64
	// Time when the event was generated. Set by the node.
	Time int64
	// Topic on which the event was published. Required.
	Topic string
	// Fields contains additional metadata associated with the event. Optional.
	Fields []Field
	// Data is the event payload.
	Data []byte
	// Codec specifies the name of the codec used to encode the event data. Optional.
	Codec string
}

type Field struct {
	Name  string
	Value any
}
