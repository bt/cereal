package cereal

import (
	"bytes"
	"testing"

	"gotest.tools/assert"
)

func TestWriter_Simple(t *testing.T) {
	buf := make([]byte, 1024)
	writer := NewWriterFromBuffer(bytes.NewBuffer(buf))

	offset, _, err := writer.Write("abc")
	assert.NilError(t, err)
	assert.Equal(t, offset, uint64(0))
	assert.DeepEqual(t, buf, []byte{0x06, 0x03, 0x61, 0x62, 0x63})

	offset, _, err = writer.Write("abc")
	assert.NilError(t, err)
	assert.Equal(t, offset, uint64(5))
	assert.DeepEqual(t, buf, []byte{0x06, 0x03, 0x61, 0x62, 0x63, 0x06, 0x03, 0x61, 0x62, 0x63})

	offset, err = writer.WriteRaw([]byte{0xFE, 0xED, 0xFA, 0xCE})
	assert.NilError(t, err)
	assert.Equal(t, offset, uint64(10))
	assert.DeepEqual(t, buf, []byte{0x06, 0x03, 0x61, 0x62, 0x63, 0x06, 0x03, 0x61, 0x62, 0x63, 0xFE, 0xED, 0xFA, 0xCE})
}

func TestWriter_Write(t *testing.T) {
	type expected struct {
		bytes   []byte
		offsets []uint64
	}
	tests := []struct {
		name     string
		data     []interface{}
		expected expected
	}{
		{
			name: "integer",
			data: []interface{}{int64(123123)},
			expected: expected{
				bytes:   []byte{0x02, 0xe6, 0x83, 0x0f},
				offsets: []uint64{0},
			},
		},
		{
			name: "unsigned integer",
			data: []interface{}{uint64(123123)},
			expected: expected{
				bytes:   []byte{0x03, 0xf3, 0xc1, 0x07},
				offsets: []uint64{0},
			},
		},
		{
			name: "string",
			data: []interface{}{"foobar"},
			expected: expected{
				bytes:   []byte{0x06, 0x06, 0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72},
				offsets: []uint64{0},
			},
		},
		{
			name: "strings",
			data: []interface{}{[]string{"foobar", "bazbar", "what the?"}},
			expected: expected{
				bytes:   []byte{0x07, 0x03, 0x06, 0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72, 0x06, 0x62, 0x61, 0x7a, 0x62, 0x61, 0x72, 0x09, 0x77, 0x68, 0x61, 0x74, 0x20, 0x74, 0x68, 0x65, 0x3f},
				offsets: []uint64{0},
			},
		},
		{
			name: "float",
			data: []interface{}{3.1415},
			expected: expected{
				bytes:   []byte{0x04, 0x40, 0x09, 0x21, 0xca, 0xc0, 0x83, 0x12, 0x6f},
				offsets: []uint64{0},
			},
		},
		{
			name: "boolean",
			data: []interface{}{true, false},
			expected: expected{
				bytes:   []byte{0x01, 0x01, 0x01, 0x00},
				offsets: []uint64{0, 1},
			},
		},
		{
			name: "multiple data sources",
			data: []interface{}{"foo", "bar", 87, true, -4, 99.11},
			expected: expected{
				bytes:   []byte{0x06, 0x03, 0x66, 0x6f, 0x6f, 0x06, 0x03, 0x62, 0x61, 0x72, 0x02, 0xae, 0x01, 0x01, 0x01, 0x02, 0x07, 0x04, 0x40, 0x58, 0xc7, 0x0a, 0x3d, 0x70, 0xa3, 0xd7},
				offsets: []uint64{0, 5, 10, 13, 14, 16},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf := make([]byte, 1024)
			writer := NewWriterFromBuffer(bytes.NewBuffer(buf))

			var offset uint64
			var err error
			for i, d := range test.data {
				offset, _, err = writer.Write(d)
				assert.NilError(t, err)
				assert.Equal(t, offset, test.expected.offsets[i])
			}

			assert.DeepEqual(t, buf, test.expected.bytes)
		})
	}
}
