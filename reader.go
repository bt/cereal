package cereal

import (
	"encoding/binary"
	"fmt"
	"io"
)

type byteSeeker struct {
	buf    []byte
	offset int64
}

func (b *byteSeeker) Read(p []byte) (n int, err error) {
	from := b.offset
	if from > int64(len(p)) {
		return 0, io.EOF
	}

	to := b.offset + int64(len(p))
	if to > int64(len(b.buf)) {
		to = int64(len(b.buf))
	}

	n = copy(p, b.buf[from:to])
	b.offset = to
	return n, nil
}

func (b *byteSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		b.offset = offset
	case io.SeekCurrent:
		b.offset += offset
	case io.SeekEnd:
		b.offset = int64(len(b.buf)) - 1 + offset
	default:
		return 0, fmt.Errorf("invalid whence")
	}

	if b.offset > int64(len(b.buf)-1) {
		return 0, io.EOF
	} else if b.offset < 0 {
		return 0, fmt.Errorf("invalid offset")
	}

	return b.offset, nil
}

type reader struct {
	r io.ReadSeeker
}

func NewReader(r io.ReadSeeker) *reader {
	return &reader{r: r}
}

func NewReaderFromBuffer(buf []byte) *reader {
	return &reader{r: &byteSeeker{buf: buf}}
}

func (r *reader) readByte() (byte, error) {
	b := make([]byte, 1)
	_, err := r.r.Read(b)
	return b[0], err
}

func (r *reader) readInt() (int64, error) {
	b := make([]byte, binary.MaxVarintLen64)
	n, err := r.r.Read(b)
	if err != nil {
		return 0, err
	}

	val, nn := binary.Varint(b)
	if nn > 0 {
		rewindBytes := n - nn
		if rewindBytes > 0 {
			_, err = r.r.Seek(-int64(rewindBytes), io.SeekCurrent)
		}
		return val, err
	}

	if nn == 0 {
		return 0, fmt.Errorf("buf too small")
	} else {
		return 0, fmt.Errorf("overflow")
	}
}

func (r *reader) readUint() (uint64, error) {
	b := make([]byte, binary.MaxVarintLen64)
	n, err := r.r.Read(b)
	if err != nil {
		return 0, err
	}

	val, nn := binary.Uvarint(b)
	if nn > 0 {
		rewindBytes := n - nn
		if rewindBytes > 0 {
			_, err = r.r.Seek(-int64(rewindBytes), io.SeekCurrent)
		}
		return val, err
	}

	if nn == 0 {
		return 0, fmt.Errorf("buf too small")
	} else {
		return 0, fmt.Errorf("overflow")
	}
}

// Read will read the next value out of the buffer.
func (r *reader) Read(expectedType DataType) (interface{}, error) {
	t, err := r.readByte()
	if err != nil {
		return nil, err
	}

	if expectedType != Any && DataType(t) != expectedType {
		return nil, fmt.Errorf("expected data type mismatch: wanted '%s', got '%s'", expectedType, DataType(t))
	}

	switch DataType(t) {
	case Integer:
		return r.readInt()
	case UnsignedInteger:
		return r.readUint()
	default:
		panic(fmt.Errorf("cannot read value, unknown data type '%v'", t))
	}
}

// ReadRaw reads data into out and returns the number of bytes read into out.
func (r *reader) ReadRaw(out []byte) (n int, err error) {
	return r.r.Read(out)
}
