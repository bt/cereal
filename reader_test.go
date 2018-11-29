package cereal

import (
	"testing"

	"gotest.tools/assert"
)

func TestReader_ReadRaw(t *testing.T) {
	buf := []byte{0xFE, 0xED, 0xFA, 0xCE}
	reader := NewReaderFromBuffer(buf)

	out := make([]byte, 4)
	n, err := reader.ReadRaw(out)
	assert.NilError(t, err)
	assert.Equal(t, n, 4)
	assert.DeepEqual(t, out, buf)
}

func TestReader_Read(t *testing.T) {
	type expected struct {
		val       interface{}
		dataType  DataType
		endOffset int64
	}

	tests := []struct {
		name     string
		buf      []byte
		expected expected
	}{
		{
			name: "integer",
			buf:  []byte{0x02, 0xe6, 0x83, 0x0f},
			expected: expected{
				val:       int64(123123),
				dataType:  Integer,
				endOffset: 4,
			},
		},
		{
			name: "extended integer",
			buf:  []byte{0x02, 0xe6, 0x83, 0x0f, 0x05, 0x00, 0x0f},
			expected: expected{
				val:       int64(123123),
				dataType:  Integer,
				endOffset: 4,
			},
		},
		{
			name: "unsigned integer",
			buf:  []byte{0x03, 0xf3, 0xc1, 0x07},
			expected: expected{
				val:       uint64(123123),
				dataType:  UnsignedInteger,
				endOffset: 4,
			},
		},
		{
			name: "float",
			buf:  []byte{0x04, 0x40, 0x09, 0x21, 0xca, 0xc0, 0x83, 0x12, 0x6f},
			expected: expected{
				val:       float64(3.1415),
				dataType:  Float,
				endOffset: 9,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := NewReaderFromBuffer(test.buf)
			i, dataType, err := reader.Read(Any)

			assert.NilError(t, err)
			assert.Equal(t, i, test.expected.val)
			assert.Equal(t, dataType, test.expected.dataType)
			assert.Equal(t, reader.r.(*byteSeeker).offset, test.expected.endOffset)
		})
	}
}
