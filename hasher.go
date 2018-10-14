package cereal

import (
	"bufio"
	"hash/crc32"
	"io"
)

type HashWriter struct {
	w   *bufio.Writer
	crc uint32
	n   int
}

// Write writes the provided bytes to the wrapped writer, recalculates the checksum and counts the bytes.
func (h *HashWriter) Write(p []byte) (n int, err error) {
	defer h.w.Flush()
	n, err = h.w.Write(p)
	h.crc = crc32.Update(h.crc, crc32.IEEETable, p[:n])
	h.n += n
	return n, err
}

func (h *HashWriter) WriteByte(b byte) (err error) {
	_, err = h.Write([]byte{b})
	return err
}

// CRC32 will return the CRC-32 hash of the written content.
func (h *HashWriter) CRC32() uint32 {
	return h.crc
}

// Count returns the number of bytes written.
func (h *HashWriter) Count() uint64 {
	return uint64(h.n)
}

// NewHashWriter returns a new HashWriter which wraps the provided writer.
func NewHashWriter(w io.Writer) *HashWriter {
	return &HashWriter{w: bufio.NewWriter(w)}
}
