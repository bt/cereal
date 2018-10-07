package cereal

// int64Value will convert the provided value to int64 otherwise panic.
func int64Value(n interface{}) int64 {
	switch n := n.(type) {
	case int:
		return int64(n)
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return int64(n)
	}
	panic("could not convert to int64")
}

// uint64Value will convert the provided value to uint64 otherwise panic.
func uint64Value(n interface{}) uint64 {
	switch n := n.(type) {
	case uint:
		return uint64(n)
	case uint8:
		return uint64(n)
	case uint16:
		return uint64(n)
	case uint32:
		return uint64(n)
	case uint64:
		return uint64(n)
	}
	panic("could not convert to uint64")
}

// floatValue will convert the provided value to a float otherwise panic.
func floatValue(n interface{}) interface{} {
	switch n := n.(type) {
	case float32:
		return float32(n)
	case float64:
		return float64(n)
	}
	panic("could not convert to float")
}
