package cereal

type DataType int

func (d DataType) String() string {
	return dataTypeStrings[d]
}

const (
	Any DataType = iota
	Boolean
	Integer
	UnsignedInteger
	Float
	Bytes
	String
	StringSlice
)

var dataTypeStrings = map[DataType]string{
	Any:             "any",
	Boolean:         "bool",
	Integer:         "int",
	UnsignedInteger: "uint",
	Float:           "float",
	Bytes:           "bytes",
	String:          "string",
	StringSlice:     "strings",
}
