package rdb

import (
	"fmt"
	"io"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

const (
	OPCODE_EOF                 = 0xFF
	OPCODE_META                = 0xFA
	OPCODE_SELECT_DB           = 0xFE
	OPCODE_HASH_TABLE_SIZES    = 0xFB
	OPCODE_EXPIRY_SECONDS      = 0xFD
	OPCODE_EXPIRY_MILLISECONDS = 0xFC
	OPCODE_STRING_VALUE        = 0x00
)

// handleOpcode processes a single opcode in the RDB file
func (p *RDBParser) handleOpcode(opcode byte) error {
	switch opcode {
	case OPCODE_EOF: // EOF marker
		// Skip checksum for now
		p.logger.Debug("Found EOF marker")
		return nil
	case OPCODE_META: // AUX field (metadata)
		return p.handleMetadata()
	case OPCODE_SELECT_DB: // Select DB
		return p.handleSelectDB()
	case OPCODE_HASH_TABLE_SIZES: // hash table sizes
		return p.handleHashTableSizes()
	case OPCODE_EXPIRY_SECONDS: // Expiry time in Seconds
		return p.handleExpirySeconds()
	case OPCODE_EXPIRY_MILLISECONDS: // Expiry time in Milliseconds
		return p.handleExpiryMilliseconds()
	case OPCODE_STRING_VALUE: // String value
		return p.handleStringValue()
	default:
		// Other value types (0-14)
		if opcode >= 0 && opcode <= 14 {
			return p.handleUnsupportedValueType(opcode)
		}
		return utils.NewRDBError("handle_opcode", fmt.Errorf("unknown opcode: %d", opcode))
	}
}

// handleMetadata processes metadata entries
func (p *RDBParser) handleMetadata() error {
	fieldName, err := readStringEncoded(p.reader)
	if err != nil {
		return utils.NewRDBError("read_metadata_name", err)
	}

	fieldValue, err := readStringEncoded(p.reader)
	if err != nil {
		return utils.NewRDBError("read_metadata_value", err)
	}

	p.logger.Info("Metadata: %s=%s", fieldName, fieldValue)
	return nil
}

// handleSelectDB processes DB selection
func (p *RDBParser) handleSelectDB() error {
	dbNum, err := readSizeEncoded(p.reader)
	if err != nil {
		return utils.NewRDBError("read_db_number", err)
	}

	p.logger.Info("Selected DB: %d", dbNum)
	return nil
}

// handleHashTableSizes processes hash table size information
func (p *RDBParser) handleHashTableSizes() error {
	keyCount, err := readSizeEncoded(p.reader)
	if err != nil {
		return utils.NewRDBError("read_key_count", err)
	}

	expireCount, err := readSizeEncoded(p.reader)
	if err != nil {
		return utils.NewRDBError("read_expire_count", err)
	}

	p.logger.Info("Hash table sizes: keys=%d, expires=%d", keyCount, expireCount)
	return nil
}

// handleExpirySeconds processes key-value pairs with expiry in seconds
func (p *RDBParser) handleExpirySeconds() error {
	expireTime := make([]byte, 4)
	if _, err := io.ReadFull(p.reader, expireTime); err != nil {
		return utils.NewRDBError("read_expiry_seconds", err)
	}

	// Convert bytes (little endian) to uint32
	expirySeconds := uint32(expireTime[0]) | uint32(expireTime[1])<<8 | uint32(expireTime[2])<<16 | uint32(expireTime[3])<<24

	p.logger.Debug("Key with expiry in seconds: %d", expirySeconds)
	return p.handleKeyValueWithExpiry(time.Unix(int64(expirySeconds), 0))
}

// handleExpiryMilliseconds processes key-value pairs with expiry in milliseconds
func (p *RDBParser) handleExpiryMilliseconds() error {
	expireTime := make([]byte, 8)
	if _, err := io.ReadFull(p.reader, expireTime); err != nil {
		return utils.NewRDBError("read_expiry_ms", err)
	}

	// Convert bytes (little-endian) to uint64
	expiryMs := uint64(expireTime[0]) | uint64(expireTime[1])<<8 |
		uint64(expireTime[2])<<16 | uint64(expireTime[3])<<24 |
		uint64(expireTime[4])<<32 | uint64(expireTime[5])<<40 |
		uint64(expireTime[6])<<48 | uint64(expireTime[7])<<56

	p.logger.Debug("Key with expiry in milliseconds: %d", expiryMs)
	return p.handleKeyValueWithExpiry(time.UnixMilli(int64(expiryMs)))
}

// handleKeyValueWithExpiry processes a key-value pair with expiry time
func (p *RDBParser) handleKeyValueWithExpiry(expireAt time.Time) error {
	valueType := make([]byte, 1)
	if _, err := io.ReadFull(p.reader, valueType); err != nil {
		return utils.NewRDBError("read_value_type", err)
	}

	// Read Key
	key, err := readStringEncoded(p.reader)
	if err != nil {
		return utils.NewRDBError("read_key", err)
	}

	// Read Value based on type
	if valueType[0] == 0 { // String
		value, err := readStringEncoded(p.reader)
		if err != nil {
			return utils.NewRDBError("read_value", err)
		}

		ttl := time.Until(expireAt)
		if ttl > 0 {
			p.logger.Info("Setting key with expiry: %s = %s (expires in %v)", key, value, ttl)
			p.store.Set(key, value, ttl)
		} else {
			p.logger.Info("Skipping expired key: %s", key)
		}
		return nil
	}

	return utils.NewRDBError("handle_value_type", fmt.Errorf("unsupported value type: %d", valueType[0]))
}

// handleStringValue processes a simple key-value string pair with no expiry
func (p *RDBParser) handleStringValue() error {
	key, err := readStringEncoded(p.reader)
	if err != nil {
		return utils.NewRDBError("read_key", err)
	}

	value, err := readStringEncoded(p.reader)
	if err != nil {
		return utils.NewRDBError("read_value", err)
	}

	p.logger.Info("Setting key: %s = %s", key, value)
	p.store.Set(key, value, 0)
	return nil
}

// handleUnsupportedValueType skips over unsupported value types
func (p *RDBParser) handleUnsupportedValueType(valueType byte) error {
	key, err := readStringEncoded(p.reader)
	if err != nil {
		return utils.NewRDBError("read_key", err)
	}

	p.logger.Info("Skipping unsupported value type %d for key %s", valueType, key)
	_, err = readStringEncoded(p.reader)
	if err != nil {
		return utils.NewRDBError("read_value", err)
	}

	return nil
}
