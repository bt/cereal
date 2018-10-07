package cereal

import (
	"fmt"
	"io"
)

type byteSeeker struct {
	buf    []byte
	offset int64
}

func (b *byteSeeker) Read(p []byte) (n int, err error) {
	from := b.offset
	if from > int64(len(p))-1 {
		return 0, io.EOF
	}

	to := b.offset + int64(len(p))
	if to > int64(len(p)) {
		to = int64(len(p)) - 1
	}

	n = copy(p, b.buf[from:to])
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
