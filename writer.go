package cereal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"reflect"

	"github.com/pierrec/lz4"
)

var (
	// LZ4 properties
	hashTable    [64 << 10]int
	lz4BlockSize = 64 << 10
)

type Writer struct {
	w                *HashWriter
	checksum         uint32
	file             *os.File
	reusableBuf      []byte
	excludeWriteType bool
}

// NewWriter will return a new writer.
func NewWriter(f *os.File) *Writer {
	return &Writer{
		w:    NewHashWriter(f),
		file: f,
	}
}

// NewBufferFromBuffer will return a new writer from a specified byte buffer.
func NewWriterFromBuffer(buf *bytes.Buffer) *Writer {
	return &Writer{
		w: NewHashWriter(buf),
	}
}

// Offset returns the current writer offset.
func (w *Writer) Offset() uint64 {
	return w.w.Count()
}

// SetExcludeWriteType will toggle whether data type enums are written to the buffer.
func (w *Writer) SetExcludeWriteType(b bool) {
	w.excludeWriteType = b
}

func (w *Writer) SeekOffset(offset uint64) error {
	if w.file != nil {
		_, err := w.file.Seek(int64(offset), io.SeekStart)
		return err
	}
	return nil
}

func (w *Writer) Write(data interface{}) (offset uint64, length int, err error) {
	offset = w.w.Count()

	switch vv := data.(type) {
	case uint, uint8, uint16, uint32, uint64:
		offset, err = w.writeUint(uint64Value(vv))
	case int, int8, int16, int32, int64:
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
	case map[string]interface{}:
		offset, err = w.writeKeyValueMap(vv)
	default:
		panic(fmt.Errorf("cannot write value, unknown data type for value: '%v' (type: %s)", vv, reflect.TypeOf(vv).String()))
	}

	if err != nil {
		return 0, 0, err
	}
	length = int(w.w.Count() - offset)
	return offset, length, err
}

// WriteRaw will write the raw bytes into the writer.
func (w *Writer) WriteRaw(buf []byte) (offset uint64, err error) {
	offset = w.w.Count()
	_, err = w.w.Write(buf)
	return offset, err
}

// WriteRawToCompress will write raw bytes to compress into LZ4, then to the writer.
func (w *Writer) WriteRawToLZ4Compress(buf []byte) (offset uint64, length int, err error) {
	currentOffset := w.w.Count()
	zbuf := make([]byte, lz4BlockSize)
	chunkData := make([]byte, lz4BlockSize)

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

		compSize, err := lz4.CompressBlock(chunkData, zbuf, hashTable[:])
		if _, err = w.WriteRaw(zbuf[0:compSize]); err != nil {
			return 0, 0, err
		}
	}

	return currentOffset, int(w.w.Count() - currentOffset), nil
}

// WriteRawByte will write a single byte into the writer.
func (w *Writer) WriteRawByte(b byte) (offset uint64, err error) {
	currentOffset := w.w.Count()
	err = w.w.WriteByte(b)
	w.checksum = crc32.Update(w.checksum, crc32.IEEETable, []byte{b})
	return currentOffset, err
}

func (w *Writer) writeUint(v uint64) (offset uint64, err error) {
	if len(w.reusableBuf) < binary.MaxVarintLen64 {
		w.reusableBuf = make([]byte, binary.MaxVarintLen64)
	}
	size := binary.PutUvarint(w.reusableBuf, v)
	offset = w.w.Count()

	// Write type
	if !w.excludeWriteType {
		if err = w.w.WriteByte(byte(UnsignedInteger)); err != nil {
			return 0, err
		}
	}

	// Write value
	if _, err = w.w.Write(w.reusableBuf[0:size]); err != nil {
		return 0, err
	}

	return offset, nil
}

func (w *Writer) writeInt(v int64) (offset uint64, err error) {
	if len(w.reusableBuf) < binary.MaxVarintLen64 {
		w.reusableBuf = make([]byte, binary.MaxVarintLen64)
	}
	size := binary.PutVarint(w.reusableBuf, v)
	offset = w.w.Count()

	// Write type
	if !w.excludeWriteType {
		if err = w.w.WriteByte(byte(Integer)); err != nil {
			return 0, err
		}
	}

	// Write value
	if _, err = w.w.Write(w.reusableBuf[0:size]); err != nil {
		return 0, err
	}

	return offset, nil
}

func (w *Writer) writeFloat(v interface{}) (offset uint64, err error) {
	offset = w.w.Count()

	// Write type
	if !w.excludeWriteType {
		if err = w.w.WriteByte(byte(Float)); err != nil {
			return 0, err
		}
	}

	// Write value
	if err = binary.Write(w.w, binary.BigEndian, v); err != nil {
		return 0, err
	}

	return offset, nil
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

	// Write bytes
	if _, err := w.w.Write(b); err != nil {
		return err
	}

	return nil
}

func (w *Writer) writeString(s string) (offset uint64, err error) {
	offset = w.w.Count()

	// Write type
	if !w.excludeWriteType {
		if err = w.w.WriteByte(byte(String)); err != nil {
			return 0, err
		}
	}

	// Write string
	if err := w.appendBytes([]byte(s)); err != nil {
		return 0, err
	}
	return offset, nil
}

func (w *Writer) writeStringSlice(s []string) (offset uint64, err error) {
	offset = w.w.Count()

	// Write type
	if !w.excludeWriteType {
		if err = w.w.WriteByte(byte(StringSlice)); err != nil {
			return 0, err
		}
	}

	// Write length of strings
	if len(w.reusableBuf) < binary.MaxVarintLen64 {
		w.reusableBuf = make([]byte, binary.MaxVarintLen64)
	}
	size := binary.PutUvarint(w.reusableBuf, uint64(len(s)))
	if _, err = w.w.Write(w.reusableBuf[0:size]); err != nil {
		return 0, err
	}

	// Write strings
	for _, ss := range s {
		if err := w.appendBytes([]byte(ss)); err != nil {
			return 0, err
		}
	}

	return offset, nil
}

func (w *Writer) writeKeyValueMap(m map[string]interface{}) (offset uint64, err error) {
	offset = w.w.Count()

	// Write type
	if !w.excludeWriteType {
		if err = w.w.WriteByte(byte(KeyValueMap)); err != nil {
			return 0, err
		}
	}
	tmpExcludeWriteType := w.excludeWriteType

	for k, v := range m {
		w.excludeWriteType = true
		if _, err = w.writeString(k); err != nil {
			return 0, err
		}

		// Value type is unknown so requires type to be written
		w.excludeWriteType = false
		if _, _, err = w.Write(v); err != nil {
			return 0, err
		}
	}

	w.excludeWriteType = tmpExcludeWriteType
	return offset, nil
}

func (w *Writer) writeBoolean(b bool) (offset uint64, err error) {
	offset = w.w.Count()

	// Write type
	if !w.excludeWriteType {
		if err = w.w.WriteByte(byte(Boolean)); err != nil {
			return 0, err
		}
	}

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
	return offset, nil
}

func (w *Writer) writeBytes(b []byte) (offset uint64, err error) {
	offset = w.w.Count()

	// Write type
	if !w.excludeWriteType {
		if err = w.w.WriteByte(byte(Bytes)); err != nil {
			return 0, err
		}
	}

	// Write bytes
	if err := w.appendBytes(b); err != nil {
		return 0, err
	}
	return offset, nil
}

// Close will close the writer.
func (w *Writer) Close() error {
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}
