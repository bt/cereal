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
				endOffset: 4,
			},
		},
		{
			name: "extended integer",
			buf:  []byte{0x02, 0xe6, 0x83, 0x0f, 0x05, 0x00, 0x0f},
			expected: expected{
				val:       int64(123123),
				endOffset: 4,
			},
		},
		{
			name: "unsigned integer",
			buf:  []byte{0x03, 0xf3, 0xc1, 0x07},
			expected: expected{
				val:       uint64(123123),
				endOffset: 4,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := NewReaderFromBuffer(test.buf)
			i, err := reader.Read(Any)

			assert.NilError(t, err)
			assert.Equal(t, i, test.expected.val)
			assert.Equal(t, reader.r.(*byteSeeker).offset, test.expected.endOffset)
		})
	}
}
