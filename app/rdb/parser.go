package rdb

import (
	"fmt"
	"io"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

// RDBParser encapsulates the RDB file parsing logic
type RDBParser struct {
	store  *store.KeyValueStore
	reader io.Reader
	logger *utils.Logger
}

// NewRDBParser creates a new RDB parser
func NewRDBParser(reader io.Reader, kvStore *store.KeyValueStore) *RDBParser {
	return &RDBParser{
		store:  kvStore,
		reader: reader,
		logger: utils.NewLogger("RDB"),
	}
}

// Parse parses the entire RDB file
func (p *RDBParser) Parse() error {
	// Read the Header (REDIS0011)
	header := make([]byte, 9)
	if _, err := io.ReadFull(p.reader, header); err != nil {
		return utils.NewRDBError("read_header", err)
	}

	if !strings.HasPrefix(string(header), "REDIS") {
		return utils.NewRDBError("validate_header", fmt.Errorf("invalid RDB file format: header doesn't start with REDIS"))
	}

	p.logger.Info("Parsing RDB file, header: %s", string(header))

	return p.parseDatabase()
}

// parseDatabase parses the database section of the RDB file
func (p *RDBParser) parseDatabase() error {
	for {
		opcode := make([]byte, 1)
		n, err := p.reader.Read(opcode)
		if err != nil {
			if err == io.EOF {
				// Reached end of file without EOF marker, which is fine
				p.logger.Debug("Reached EOF without marker")
				return nil
			}
			return utils.NewRDBError("read_opcode", err)
		}

		if n == 0 {
			// No bytes read but no error, this is strange but we'll treat it as EOF
			p.logger.Debug("Read 0 bytes without error, treating as EOF")
			return nil
		}

		p.logger.Debug("Processing opcode: 0x%02X", opcode[0])

		// Handle each opcode
		if err := p.handleOpcode(opcode[0]); err != nil {
			if utils.IsEOF(err) {
				// EOF during handling is fine
				p.logger.Debug("EOF during opcode handling")
				return nil
			}
			return err
		}
	}
}
