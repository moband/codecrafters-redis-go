package rdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
)

// readSizeEncoded reads a size-encoded value according to the RDB format
func readSizeEncoded(r io.Reader) (int, error) {
	firstByte := make([]byte, 1)
	if _, err := io.ReadFull(r, firstByte); err != nil {
		return 0, err
	}

	// Check the first two bits to determine the encoding
	prefix := (firstByte[0] & 0xC0) >> 6

	switch prefix {
	case 0: // 00 - 6 bit length
		return readSize6Bit(firstByte[0]), nil
	case 1: // 01 - 14 bit length
		return readSize14Bit(r, firstByte[0])
	case 2: // 10 - 32 bit length
		return readSize32Bit(r)
	case 3: // 11 - special encoding for integers
		return readSpecialEncoding(r, firstByte[0])
	}

	return 0, fmt.Errorf("invalid size encoding")
}

// readSize6Bit reads a 6-bit length value
func readSize6Bit(firstByte byte) int {
	return int(firstByte & 0x3F)
}

// readSize14Bit reads a 14-bit length value
func readSize14Bit(r io.Reader, firstByte byte) (int, error) {
	secondByte := make([]byte, 1)
	if _, err := io.ReadFull(r, secondByte); err != nil {
		return 0, err
	}
	return int(int(firstByte&0x3F)<<8 | int(secondByte[0])), nil
}

// readSize32Bit reads a 32-bit length value
func readSize32Bit(r io.Reader) (int, error) {
	data := make([]byte, 4)
	if _, err := io.ReadFull(r, data); err != nil {
		return 0, err
	}
	// Use binary.BigEndian to read the integer
	return int(binary.BigEndian.Uint32(data)), nil
}

// readSpecialEncoding handles special encoding types (integers)
func readSpecialEncoding(r io.Reader, firstByte byte) (int, error) {
	encoding := firstByte & 0x3F

	switch encoding {
	case 0: // 8 bit integer
		return read8BitInteger(r)
	case 1: // 16 bit integer
		return read16BitInteger(r)
	case 2: // 32 bit integer
		return read32BitInteger(r)
	default:
		return 0, fmt.Errorf("unsupported special encoding: %d", encoding)
	}
}

// read8BitInteger reads an 8-bit integer
func read8BitInteger(r io.Reader) (int, error) {
	data := make([]byte, 1)
	if _, err := io.ReadFull(r, data); err != nil {
		return 0, err
	}
	return -int(data[0]), nil
}

// read16BitInteger reads a 16-bit integer
func read16BitInteger(r io.Reader) (int, error) {
	data := make([]byte, 2)
	if _, err := io.ReadFull(r, data); err != nil {
		return 0, err
	}
	// Use binary.LittleEndian to read the integer
	return -int(binary.LittleEndian.Uint16(data)), nil
}

// read32BitInteger reads a 32-bit integer
func read32BitInteger(r io.Reader) (int, error) {
	data := make([]byte, 4)
	if _, err := io.ReadFull(r, data); err != nil {
		return 0, err
	}
	// Use binary.LittleEndian to read the integer
	return -int(binary.LittleEndian.Uint32(data)), nil
}

// readStringEncoded reads a string-encoded value
func readStringEncoded(r io.Reader) (string, error) {
	length, err := readSizeEncoded(r)
	if err != nil {
		return "", err
	}

	// If length is negative, it's a special encoding for integers
	if length < 0 {
		return strconv.Itoa(-length), nil
	}

	// Regular string data
	return readStringData(r, length)
}

// readStringData reads a raw string of the given length
func readStringData(r io.Reader, length int) (string, error) {
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return "", err
	}
	return string(data), nil
}
