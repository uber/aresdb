//  Copyright (c) 2017-2018 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"bytes"
	"unsafe"
)

// AlignOffset returns the next offset given a specific alignment.
func AlignOffset(offset int, alignment int) int {
	if alignment > 1 {
		remainder := offset % alignment
		if remainder != 0 {
			return offset + alignment - remainder
		}
	}
	return offset
}

// BufferReader provides read function for different type to read from the underline buffer.
type BufferReader struct {
	buffer []byte
}

// NewBufferReader creates a BufferReader.
func NewBufferReader(buffer []byte) BufferReader {
	return BufferReader{buffer: buffer}
}

// ReadUint8 reads 1 byte from buffer.
func (b BufferReader) ReadUint8(offset int) (uint8, error) {
	if offset+1 > len(b.buffer) {
		return 0, StackError(nil, "Failed to read uint8 from offset %d", offset)
	}
	return *(*uint8)(unsafe.Pointer(&b.buffer[offset])), nil
}

// ReadInt8 reads 1 byte from buffer.
func (b BufferReader) ReadInt8(offset int) (int8, error) {
	if offset+1 > len(b.buffer) {
		return 0, StackError(nil, "Failed to read int8 from offset %d", offset)
	}
	return *(*int8)(unsafe.Pointer(&b.buffer[offset])), nil
}

// ReadUint16 reads 2 bytes from buffer.
func (b BufferReader) ReadUint16(offset int) (uint16, error) {
	if offset+2 > len(b.buffer) {
		return 0, StackError(nil, "Failed to read uint16 from offset %d", offset)
	}
	return *(*uint16)(unsafe.Pointer(&b.buffer[offset])), nil
}

// ReadInt16 reads 2 bytes from buffer.
func (b BufferReader) ReadInt16(offset int) (int16, error) {
	if offset+2 > len(b.buffer) {
		return 0, StackError(nil, "Failed to read int16 from offset %d", offset)
	}
	return *(*int16)(unsafe.Pointer(&b.buffer[offset])), nil
}

// ReadUint32 reads 4 bytes from buffer.
func (b BufferReader) ReadUint32(offset int) (uint32, error) {
	if offset+4 > len(b.buffer) {
		return 0, StackError(nil, "Failed to read uint32 from offset %d", offset)
	}
	return *(*uint32)(unsafe.Pointer(&b.buffer[offset])), nil
}

// ReadInt32 reads 4 bytes from buffer.
func (b BufferReader) ReadInt32(offset int) (int32, error) {
	if offset+4 > len(b.buffer) {
		return 0, StackError(nil, "Failed to read int32 from offset %d", offset)
	}
	return *(*int32)(unsafe.Pointer(&b.buffer[offset])), nil
}

// ReadUint64 reads 8 bytes from buffer.
func (b BufferReader) ReadUint64(offset int) (uint64, error) {
	if offset+8 > len(b.buffer) {
		return 0, StackError(nil, "Failed to read uint64 from offset %d", offset)
	}
	return *(*uint64)(unsafe.Pointer(&b.buffer[offset])), nil
}

// ReadInt64 reads 8 bytes from buffer.
func (b BufferReader) ReadInt64(offset int) (int64, error) {
	if offset+8 > len(b.buffer) {
		return 0, StackError(nil, "Failed to read int64 from offset %d", offset)
	}
	return *(*int64)(unsafe.Pointer(&b.buffer[offset])), nil
}

// ReadFloat32 reads 4 bytes from buffer.
func (b BufferReader) ReadFloat32(offset int) (float32, error) {
	if offset+4 > len(b.buffer) {
		return 0, StackError(nil, "Failed to read float32 from offset %d", offset)
	}
	return *(*float32)(unsafe.Pointer(&b.buffer[offset])), nil
}

// BufferWriter provides functions to write different data types into the underline buffer. It
// supports both random access and sequential access.
type BufferWriter struct {
	offset    int
	bitOffset int
	buffer    []byte
}

// NewBufferWriter creates a new buffer writer over a buffer.
func NewBufferWriter(buffer []byte) BufferWriter {
	return BufferWriter{buffer: buffer}
}

// GetOffset returns the current offset value.
func (b BufferWriter) GetOffset() int {
	return b.offset
}

// AlignBytes aligns the offset/bit offset to the next byte specified in alignment.
// The new offset may cross the buffer boundary.
func (b *BufferWriter) AlignBytes(alignment int) {
	if b.bitOffset > 0 {
		b.offset++
		b.bitOffset = 0
	}
	b.offset = AlignOffset(b.offset, alignment)
}

// SkipBytes moves the underline offset by specified bytes.
// The new offset may cross the buffer boundary.
func (b *BufferWriter) SkipBytes(bytes int) {
	b.AlignBytes(1)
	b.offset += bytes
}

// SkipBits moves the bit offset and possibly byte offset.
// The new offset may cross the buffer boundary.
func (b *BufferWriter) SkipBits(bits int) {
	b.bitOffset += bits
	b.offset += b.bitOffset / 8
	b.bitOffset = b.bitOffset % 8
}

// AppendBool append a boolean value to buffer and advances byte/bit offset.
func (b *BufferWriter) AppendBool(value bool) error {
	if b.offset >= len(b.buffer) {
		return StackError(nil, "Failed to write bool to buffer at offset (%d,%d)", b.offset, b.bitOffset)
	}
	// Need to advance to the next byte.
	if value {
		b.buffer[b.offset] |= 0x1 << uint8(b.bitOffset)
	} else {
		b.buffer[b.offset] &^= 0x1 << uint8(b.bitOffset)
	}
	if b.bitOffset == 7 {
		b.offset++
		b.bitOffset = 0
	} else {
		b.bitOffset++
	}
	return nil
}

// AppendInt8 writes a int8 value to buffer and advances offset.
func (b *BufferWriter) AppendInt8(value int8) error {
	b.AlignBytes(1)
	if err := b.WriteInt8(value, b.offset); err != nil {
		return err
	}
	b.offset++
	return nil
}

// AppendUint8 writes a uint8 value to buffer and advances offset.
func (b *BufferWriter) AppendUint8(value uint8) error {
	b.AlignBytes(1)
	if err := b.WriteUint8(value, b.offset); err != nil {
		return err
	}
	b.offset++
	return nil
}

// AppendInt16 writes a int16 value to buffer and advances offset.
func (b *BufferWriter) AppendInt16(value int16) error {
	b.AlignBytes(1)
	if err := b.WriteInt16(value, b.offset); err != nil {
		return err
	}
	b.offset += 2
	return nil
}

// AppendUint16 writes a uint16 value to buffer and advances offset.
func (b *BufferWriter) AppendUint16(value uint16) error {
	b.AlignBytes(1)
	if err := b.WriteUint16(value, b.offset); err != nil {
		return err
	}
	b.offset += 2
	return nil
}

// AppendInt32 writes a int32 value to buffer and advances offset.
func (b *BufferWriter) AppendInt32(value int32) error {
	b.AlignBytes(1)
	if err := b.WriteInt32(value, b.offset); err != nil {
		return err
	}
	b.offset += 4
	return nil
}

// AppendInt64 writes a int64 value to buffer and advances offset.
func (b *BufferWriter) AppendInt64(value int64) error {
	b.AlignBytes(1)
	if err := b.WriteInt64(value, b.offset); err != nil {
		return err
	}
	b.offset += 8
	return nil
}

// AppendUint32 writes a uint32 value to buffer and advances offset.
func (b *BufferWriter) AppendUint32(value uint32) error {
	b.AlignBytes(1)
	if err := b.WriteUint32(value, b.offset); err != nil {
		return err
	}
	b.offset += 4
	return nil
}

// AppendUint64 writes a uint64 value to buffer and advances offset.
func (b *BufferWriter) AppendUint64(value uint64) error {
	b.AlignBytes(1)
	if err := b.WriteUint64(value, b.offset); err != nil {
		return err
	}
	b.offset += 8
	return nil
}

// AppendFloat32 writes a float32 value to buffer and advances offset.
func (b *BufferWriter) AppendFloat32(value float32) error {
	b.AlignBytes(1)
	if err := b.WriteFloat32(value, b.offset); err != nil {
		return err
	}
	b.offset += 4
	return nil
}

// Append writes a byte slice to buffer and advances offset.
func (b *BufferWriter) Append(bs []byte) error {
	b.AlignBytes(1)
	if _, err := b.WriteAt(bs, int64(b.offset)); err != nil {
		return err
	}
	b.offset += len(bs)
	return nil
}

// WriteInt8 writes a int8 value to buffer at given offset.
func (b *BufferWriter) WriteInt8(value int8, offset int) error {
	if offset+1 > len(b.buffer) {
		return StackError(nil, "Failed to write int8 to buffer at offset %d", offset)
	}
	*(*int8)(unsafe.Pointer(&b.buffer[offset])) = value
	return nil
}

// WriteUint8 writes a uint8 value to buffer and advances offset.
func (b *BufferWriter) WriteUint8(value uint8, offset int) error {
	if offset+1 > len(b.buffer) {
		return StackError(nil, "Failed to write uint8 to buffer at offset %d", offset)
	}
	*(*uint8)(unsafe.Pointer(&b.buffer[offset])) = value
	return nil
}

// WriteInt16 writes a int16 value to buffer and advances offset.
func (b *BufferWriter) WriteInt16(value int16, offset int) error {
	if offset+2 > len(b.buffer) {
		return StackError(nil, "Failed to write int16 to buffer at offset %d", offset)
	}
	*(*int16)(unsafe.Pointer(&b.buffer[offset])) = value
	return nil
}

// WriteUint16 writes a uint16 value to buffer and advances offset.
func (b *BufferWriter) WriteUint16(value uint16, offset int) error {
	if offset+2 > len(b.buffer) {
		return StackError(nil, "Failed to write uint16 to buffer at offset %d", offset)
	}
	*(*uint16)(unsafe.Pointer(&b.buffer[offset])) = value
	return nil
}

// WriteInt32 writes a int32 value to buffer and advances offset.
func (b *BufferWriter) WriteInt32(value int32, offset int) error {
	if offset+4 > len(b.buffer) {
		return StackError(nil, "Failed to write int32 to buffer at offset %d", offset)
	}
	*(*int32)(unsafe.Pointer(&b.buffer[offset])) = value
	return nil
}

// WriteInt64 writes a int64 value to buffer and advances offset.
func (b *BufferWriter) WriteInt64(value int64, offset int) error {
	if offset+8 > len(b.buffer) {
		return StackError(nil, "Failed to write int64 to buffer at offset %d", offset)
	}
	*(*int64)(unsafe.Pointer(&b.buffer[offset])) = value
	return nil
}

// WriteUint32 writes a uint32 value to buffer and advances offset.
func (b *BufferWriter) WriteUint32(value uint32, offset int) error {
	if offset+4 > len(b.buffer) {
		return StackError(nil, "Failed to write uint32 to buffer at offset %d", offset)
	}
	*(*uint32)(unsafe.Pointer(&b.buffer[offset])) = value
	return nil
}

// WriteUint64 writes a uint64 value to buffer and advances offset.
func (b *BufferWriter) WriteUint64(value uint64, offset int) error {
	if offset+8 > len(b.buffer) {
		return StackError(nil, "Failed to write uint64 to buffer at offset %d", offset)
	}
	*(*uint64)(unsafe.Pointer(&b.buffer[offset])) = value
	return nil
}

// WriteFloat32 writes a float32 value to buffer and advances offset.
func (b *BufferWriter) WriteFloat32(value float32, offset int) error {
	if offset+4 > len(b.buffer) {
		return StackError(nil, "Failed to write float32 to buffer at offset %d", offset)
	}
	*(*float32)(unsafe.Pointer(&b.buffer[offset])) = value
	return nil
}

// Write implements Write in io.Writer interface
func (b *BufferWriter) Write(bs []byte) (int, error) {
	err := b.Append(bs)
	if err != nil {
		return 0, err
	}
	return len(bs), nil
}

// WriteAt implements WriteAt in io.WriterAt interface
func (b *BufferWriter) WriteAt(bs []byte, offset int64) (int, error) {
	if offset+int64(len(bs)) > int64(len(b.buffer)) {
		return 0, StackError(nil, "Failed to write []byte to buffer at offset %d", offset)
	}
	copy(b.buffer[offset:], bs)
	return len(bs), nil
}

// ClosableBuffer is not really closable but just implements the utils.WriteSyncCloser interface.
type ClosableBuffer struct {
	*bytes.Buffer
}

// Close just implements Close function of utils.WriteSyncCloser interface.
func (cb *ClosableBuffer) Close() error {
	return nil
}

// ClosableReader is not really closable but just implements the utils.WriteSyncCloser interface.
type ClosableReader struct {
	*bytes.Reader
}

// Close just implements Close function of utils.WriteSyncCloser interface.
func (cr *ClosableReader) Close() error {
	return nil
}

// Sync just implements Sync function of utils.WriteSyncCloser interface.
func (cr *ClosableBuffer) Sync() error {
	return nil
}
