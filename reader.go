package cereal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/pierrec/lz4"
)

type byteSeeker struct {
	buf    []byte
	offset int64
}

func (b *byteSeeker) Read(p []byte) (n int, err error) {
	from := b.offset
	if from >= int64(len(b.buf)) {
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

type Reader struct {
	r io.ReadSeeker
}

func NewReader(r io.ReadSeeker) *Reader {
	return &Reader{r: r}
}

func NewReaderFromBuffer(buf []byte) *Reader {
	return &Reader{r: &byteSeeker{buf: buf}}
}

func (r *Reader) readByte() (byte, error) {
	b := make([]byte, 1)
	_, err := r.r.Read(b)
	return b[0], err
}

func (r *Reader) readBytes(buf []byte) (err error) {
	_, err = r.r.Read(buf)
	return err
}

func (r *Reader) readString() (string, DataType, error) {
	len, _, err := r.readUint()
	if err != nil {
		return "", String, err
	}

	str := make([]byte, len)
	if err = r.readBytes(str); err != nil {
		return "", String, err
	}
	return string(str), String, nil
}

func (r *Reader) readInt() (int64, DataType, error) {
	b := make([]byte, binary.MaxVarintLen64)
	n, err := r.r.Read(b)
	if err != io.EOF && err != nil {
		return 0, Integer, err
	}

	val, nn := binary.Varint(b)
	if nn > 0 {
		rewindBytes := n - nn
		if rewindBytes > 0 {
			_, err = r.r.Seek(-int64(rewindBytes), io.SeekCurrent)
		}
		return val, Integer, err
	}

	if nn == 0 {
		return 0, Integer, fmt.Errorf("buf too small")
	} else {
		return 0, Integer, fmt.Errorf("overflow")
	}
}

func (r *Reader) readUint() (uint64, DataType, error) {
	b := make([]byte, binary.MaxVarintLen64)
	n, err := r.r.Read(b)
	if err != io.EOF && err != nil {
		return 0, UnsignedInteger, err
	}

	val, nn := binary.Uvarint(b)
	if nn > 0 {
		rewindBytes := n - nn
		if rewindBytes > 0 {
			_, err = r.r.Seek(-int64(rewindBytes), io.SeekCurrent)
		}
		return val, UnsignedInteger, err
	}

	if nn == 0 {
		return 0, UnsignedInteger, fmt.Errorf("buf too small")
	} else {
		return 0, UnsignedInteger, fmt.Errorf("overflow")
	}
}

func (r *Reader) readKeyValueMap() (map[string]interface{}, DataType, error) {
	m := make(map[string]interface{})

	// Read length
	len, _, err := r.readUint()
	if err != nil {
		return nil, KeyValueMap, err
	}

	for i := uint64(0); i < len; i++ {
		// Read key
		key, _, err := r.readString()
		if err != nil {
			return nil, KeyValueMap, err
		}

		// Read value
		val, _, err := r.Read(Any)
		if err != nil {
			return nil, KeyValueMap, err
		}

		m[key] = val
	}

	return m, KeyValueMap, nil
}

// Read will read the next value out of the buffer.
func (r *Reader) Read(expectedType DataType) (interface{}, DataType, error) {
	t, err := r.readByte()
	if err != nil {
		return nil, 0, err
	}

	if expectedType != Any && DataType(t) != expectedType {
		return nil, 0, fmt.Errorf("expected data type mismatch: wanted '%s', got '%s'", expectedType, DataType(t))
	}

	return r.ReadGivenType(DataType(t))
}

// ReadRaw reads data into out and returns the number of bytes read into out.
func (r *Reader) ReadRaw(out []byte) (n int, err error) {
	return r.r.Read(out)
}

// ReadGivenType will read the next value given the type.
func (r *Reader) ReadGivenType(givenType DataType) (interface{}, DataType, error) {
	switch givenType {
	case Byte:
		val, err := r.readByte()
		return val, givenType, err
	case Bytes:
		len, _, err := r.readUint()
		if err != nil {
			return nil, Bytes, err
		}
		buf := make([]byte, len)
		err = r.readBytes(buf)
		return buf, Bytes, err
	case String:
		return r.readString()
	case Integer:
		return r.readInt()
	case UnsignedInteger:
		return r.readUint()
	case Boolean:
		val, err := r.readByte()
		return val != 0, givenType, err
	case KeyValueMap:
		return r.readKeyValueMap()
	default:
		panic(fmt.Errorf("cannot read value, unknown data type '%v'", givenType))
	}
}

// ReadCompressedBlock will read the next block and decompress it into out.
func (r *Reader) ReadCompressedBlock(out []byte) (err error) {
	buf := make([]byte, lz4BlockSize)
	_, err = r.r.Read(buf)
	if err != nil {
		return err
	}
	_, err = lz4.UncompressBlock(buf, out)
	if err != nil {
		return err
	}
	return nil
}

// DecompressToFile will read in the entire reader buffer and decompress it to the specified file.
func (r *Reader) DecompressToFile(filePath string) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	zr := lz4.NewReader(r.r)
	var decomp bytes.Buffer
	_, err = io.Copy(&decomp, zr)
	if err != nil {
		return err
	}

	_, err = f.Write(decomp.Bytes())
	if err != nil {
		return err
	}
	return nil
}
