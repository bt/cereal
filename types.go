package cereal

type dataType int

const (
	Boolean dataType = iota
	Integer
	UnsignedInteger
	Float
	Bytes
	String
	StringSlice
)