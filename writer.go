package cereal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"reflect"

	"github.com/pierrec/lz4"
)

type Writer struct {
	w             *bufio.Writer
	file          *os.File
	currentOffset uint64
	reusableBuf   []byte
}

// NewWriter will return a new writer.
func NewWriter(f *os.File) *Writer {
	return &Writer{w: bufio.NewWriter(f), file: f}
}

// NewBufferFromBuffer will return a new writer from a specified byte buffer.
func NewWriterFromBuffer(buf *bytes.Buffer) *Writer {
	return &Writer{w: bufio.NewWriter(buf)}
}

func (w *Writer) SeekOffset(offset uint64) error {
	if w.file != nil {
		_, err := w.file.Seek(int64(offset), io.SeekStart)
		return err
	}
	w.currentOffset = offset
	return nil
}

func (w *Writer) Write(data interface{}) (offset uint64, length uint64, err error) {
	currentOffset := w.currentOffset

	switch vv := data.(type) {
	case uint, uint8, uint32, uint64:
		offset, err = w.writeUint(uint64Value(vv))
	case int, int8, int32, int64:
		offset, err = w.writeInt(int64Value(vv))
	case float32, float64:
		offset, err = w.writeFloat(floatValue(vv))
	case []byte:
		offset, err = w.writeBytes(vv)
	case string:
		offset, err = w.writeString(vv)
	case []string:
		offset, err = w.writeStringSlice(vv)
	case bool:
		offset, err = w.writeBoolean(vv)
	default:
		panic(fmt.Errorf("cannot write value, unknown data type for value: '%v' (type: %s)", vv, reflect.TypeOf(vv).String()))
	}

	if err != nil {
		return 0, 0, err
	}
	err = w.w.Flush()

	length = w.currentOffset - currentOffset
	return offset, length, err
}

// WriteRaw will write the raw bytes into the writer.
func (w *Writer) WriteRaw(buf []byte) (offset uint64, err error) {
	defer w.w.Flush()
	currentOffset := w.currentOffset

	var nn int
	nn, err = w.w.Write(buf)

	w.currentOffset += uint64(nn)
	return currentOffset, err
}

// WriteRawToCompress will write raw bytes to compress into LZ4, then to the writer.
func (w *Writer) WriteRawToLZ4Compress(buf []byte) (offset uint64, length int, err error) {
	var ht [1 << 16]int
	currentOffset := w.currentOffset

	// 64 KB blocks
	zbuf := make([]byte, 64<<10)
	chunkData := make([]byte, 64<<10)

	r := bytes.NewReader(buf)
	for {
		// Read chunk
		n, err := r.Read(chunkData)
		if err != nil && err != io.EOF {
			return 0, 0, err
		}
		if n == 0 {
			break
		}

		compSize, err := lz4.CompressBlock(chunkData, zbuf, ht[:])
		if _, err = w.WriteRaw(zbuf[0:compSize]); err != nil {
			return 0, 0, err
		}
	}

	return offset, int(w.currentOffset - currentOffset), nil
}

// WriteRawByte will write a single byte into the writer.
func (w *Writer) WriteRawByte(b byte) (offset uint64, err error) {
	defer w.w.Flush()
	currentOffset := w.currentOffset
	err = w.w.WriteByte(b)
	w.currentOffset++
	return currentOffset, err
}

func (w *Writer) writeUint(v uint64) (offset uint64, err error) {
	if len(w.reusableBuf) < binary.MaxVarintLen64 {
		w.reusableBuf = make([]byte, binary.MaxVarintLen64)
	}
	size := binary.PutUvarint(w.reusableBuf, v)
	startOffset := w.currentOffset

	// Write type
	if err = w.w.WriteByte(byte(UnsignedInteger)); err != nil {
		return 0, err
	}
	w.currentOffset++

	// Write value
	if _, err = w.w.Write(w.reusableBuf[0:size]); err != nil {
		return 0, err
	}
	w.currentOffset += uint64(size)

	return startOffset, nil
}

func (w *Writer) writeInt(v int64) (offset uint64, err error) {
	if len(w.reusableBuf) < binary.MaxVarintLen64 {
		w.reusableBuf = make([]byte, binary.MaxVarintLen64)
	}
	size := binary.PutVarint(w.reusableBuf, v)
	startOffset := w.currentOffset

	// Write type
	if err = w.w.WriteByte(byte(Integer)); err != nil {
		return 0, err
	}
	w.currentOffset++

	// Write value
	if _, err = w.w.Write(w.reusableBuf[0:size]); err != nil {
		return 0, err
	}
	w.currentOffset += uint64(size)

	return startOffset, nil
}

func (w *Writer) writeFloat(v interface{}) (offset uint64, err error) {
	startOffset := w.currentOffset

	// Write type
	if err = w.w.WriteByte(byte(Float)); err != nil {
		return 0, err
	}
	w.currentOffset++

	// Write value
	if err = binary.Write(w.w, binary.BigEndian, v); err != nil {
		return 0, err
	}
	w.currentOffset += 8

	return startOffset, nil
}

func (w *Writer) appendBytes(b []byte) (err error) {
	if len(w.reusableBuf) < binary.MaxVarintLen64 {
		w.reusableBuf = make([]byte, binary.MaxVarintLen64)
	}

	// Write length
	size := binary.PutUvarint(w.reusableBuf, uint64(len(b)))
	if _, err = w.w.Write(w.reusableBuf[0:size]); err != nil {
		return err
	}
	w.currentOffset += uint64(size)

	// Write bytes
	if _, err := w.w.Write(b); err != nil {
		return err
	}
	w.currentOffset += uint64(len(b))

	return nil
}

func (w *Writer) writeString(s string) (offset uint64, err error) {
	startOffset := w.currentOffset

	// Write type
	if err = w.w.WriteByte(byte(String)); err != nil {
		return 0, err
	}
	w.currentOffset++

	// Write string
	if err := w.appendBytes([]byte(s)); err != nil {
		return 0, err
	}
	return startOffset, nil
}

func (w *Writer) writeStringSlice(s []string) (offset uint64, err error) {
	startOffset := w.currentOffset

	// Write type
	if err = w.w.WriteByte(byte(StringSlice)); err != nil {
		return 0, err
	}
	w.currentOffset++

	// Write length of strings
	if len(w.reusableBuf) < binary.MaxVarintLen64 {
		w.reusableBuf = make([]byte, binary.MaxVarintLen64)
	}
	size := binary.PutUvarint(w.reusableBuf, uint64(len(s)))
	if _, err = w.w.Write(w.reusableBuf[0:size]); err != nil {
		return 0, err
	}
	w.currentOffset += uint64(size)

	// Write strings
	for _, ss := range s {
		if err := w.appendBytes([]byte(ss)); err != nil {
			return 0, err
		}
	}

	return startOffset, nil
}

func (w *Writer) writeBoolean(b bool) (offset uint64, err error) {
	startOffset := w.currentOffset

	// Write type
	if err = w.w.WriteByte(byte(Boolean)); err != nil {
		return 0, err
	}
	w.currentOffset++

	// Write value
	if b {
		if err = w.w.WriteByte(1); err != nil {
			return 0, err
		}
	} else {
		if err = w.w.WriteByte(0); err != nil {
			return 0, err
		}
	}
	return startOffset, nil
}

func (w *Writer) writeBytes(b []byte) (offset uint64, err error) {
	startOffset := w.currentOffset

	// Write type
	if err = w.w.WriteByte(byte(Bytes)); err != nil {
		return 0, err
	}
	w.currentOffset++

	// Write bytes
	if err := w.appendBytes(b); err != nil {
		return 0, err
	}
	return startOffset, nil
}

// Close will close the writer.
func (w *Writer) Close() error {
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}
