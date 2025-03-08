package commands

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
	"github.com/codecrafters-io/redis-starter-go/app/replication"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

var logger = utils.NewLogger("Commands")

// Config represents server configuration
type Config struct {
	Role       string
	MasterHost string
	MasterPort string
	Port       string
	Dir        string
	DBFilename string
}

// CommandHandler handles execution of Redis commands
type CommandHandler struct {
	store  *store.KeyValueStore
	config Config
}

// NewCommandHandler creates a new command handler
func NewCommandHandler(kvStore *store.KeyValueStore, config Config) *CommandHandler {
	return &CommandHandler{
		store:  kvStore,
		config: config,
	}
}

// ExecuteCommand processes a Redis command and returns a response
func (h *CommandHandler) ExecuteCommand(resp protocol.RESP, conn net.Conn) (protocol.RESP, error) {
	if resp.Type != protocol.RESP_ARRAY || len(resp.Elements) == 0 {
		return protocol.RESP{}, fmt.Errorf("invalid command format")
	}

	commandResp := resp.Elements[0]
	if commandResp.Type != protocol.RESP_BULK_STRING {
		return protocol.RESP{}, fmt.Errorf("command must be a bulk string")
	}

	command := strings.ToUpper(commandResp.Str)

	// For debugging
	logger.Debug("Received command: %s", command)

	switch command {
	case "PING":
		return protocol.RESP{Type: protocol.RESP_SIMPLE_STRING, Str: "PONG"}, nil

	case "ECHO":
		if len(resp.Elements) < 2 {
			return protocol.RESP{}, fmt.Errorf("wrong number of arguments for 'echo' command")
		}

		return resp.Elements[1], nil

	case "SET":
		if len(resp.Elements) < 3 {
			return protocol.RESP{}, fmt.Errorf("wrong number of arguments for 'set' command")
		}

		key := resp.Elements[1].Str
		value := resp.Elements[2].Str

		expiry := time.Duration(0)

		for i := 3; i < len(resp.Elements)-1; i++ {
			option := strings.ToUpper(resp.Elements[i].Str)
			if option == "PX" && i+1 < len(resp.Elements) {
				pxValue := resp.Elements[i+1].Str
				ms, err := strconv.Atoi(pxValue)
				if err != nil {
					return protocol.RESP{}, fmt.Errorf("invalid expire time in 'set' command")
				}
				expiry = time.Duration(ms) * time.Millisecond
				break
			}
		}

		h.store.Set(key, value, expiry)

		return protocol.RESP{Type: protocol.RESP_SIMPLE_STRING, Str: "OK"}, nil

	case "GET":
		if len(resp.Elements) < 2 {
			return protocol.RESP{}, fmt.Errorf("wrong number of arguments for 'get' command")
		}

		key := resp.Elements[1].Str

		value, exists := h.store.Get(key)

		if !exists {
			return protocol.RESP{Type: protocol.RESP_BULK_STRING, Str: "", Num: -1}, nil
		}

		return protocol.RESP{Type: protocol.RESP_BULK_STRING, Str: value}, nil

	case "CONFIG":
		return h.handleConfigCommand(resp)

	case "KEYS":
		return h.handleKeysCommand(resp)

	case "INFO":
		return h.handleInfoCommand(resp)

	case "REPLCONF":
		return h.handleReplconfCommand(resp, conn)

	case "PSYNC":
		return h.handlePsyncCommand(resp, conn)

	case "WAIT":
		return h.handleWaitCommand(resp)

	default:
		return protocol.RESP{}, fmt.Errorf("unknown command '%s'", commandResp.Str)
	}
}

// handleConfigCommand handles the CONFIG command
func (h *CommandHandler) handleConfigCommand(resp protocol.RESP) (protocol.RESP, error) {
	if len(resp.Elements) < 3 {
		return protocol.RESP{}, fmt.Errorf("wrong number of arguments for 'config' command")
	}

	subcommand := strings.ToUpper(resp.Elements[1].Str)

	switch subcommand {
	case "GET":
		param := resp.Elements[2].Str
		var value string
		var exists bool

		switch strings.ToLower(param) {
		case "dir":
			value, exists = h.config.Dir, true
		case "dbfilename":
			value, exists = h.config.DBFilename, true
		case "port":
			value, exists = h.config.Port, true
		default:
			exists = false
		}

		if !exists {
			return protocol.RESP{Type: protocol.RESP_ARRAY, Elements: []protocol.RESP{}}, nil
		}

		return protocol.RESP{
			Type: protocol.RESP_ARRAY,
			Elements: []protocol.RESP{
				{Type: protocol.RESP_BULK_STRING, Str: param},
				{Type: protocol.RESP_BULK_STRING, Str: value},
			},
		}, nil
	}

	return protocol.RESP{}, fmt.Errorf("unknown CONFIG subcommand '%s'", subcommand)
}

// handleKeysCommand handles the KEYS command
func (h *CommandHandler) handleKeysCommand(resp protocol.RESP) (protocol.RESP, error) {
	if len(resp.Elements) < 2 {
		return protocol.RESP{}, fmt.Errorf("wrong number of arguments for 'keys' command")
	}

	pattern := resp.Elements[1].Str

	if pattern != "*" {
		return protocol.RESP{}, fmt.Errorf("only '*' pattern is supported for 'keys' command")
	}

	keys := h.store.GetAllKeys()
	elements := make([]protocol.RESP, len(keys))
	for i, key := range keys {
		elements[i] = protocol.RESP{Type: protocol.RESP_BULK_STRING, Str: key}
	}

	return protocol.RESP{Type: protocol.RESP_ARRAY, Elements: elements}, nil
}

// handleInfoCommand handles the INFO command
func (h *CommandHandler) handleInfoCommand(resp protocol.RESP) (protocol.RESP, error) {
	section := ""
	if len(resp.Elements) > 1 {
		section = strings.ToLower(resp.Elements[1].Str)
	}

	var info string
	if section == "" {
		// Full INFO command - include all sections
		info = h.getAllInfoSections()
	} else if section == "replication" {
		// Just the replication section
		info = h.getReplicationInfo()
	} else {
		// Handle unknown section
		info = fmt.Sprintf("# %s\n", strings.Title(section))
	}

	// Return as RESP bulk string
	return protocol.RESP{Type: protocol.RESP_BULK_STRING, Str: info}, nil
}

// getReplicationInfo returns information about replication
func (h *CommandHandler) getReplicationInfo() string {
	info := "# Replication\n"

	if h.config.Role == "slave" {
		// Slave role
		slaveInfo := replication.GetReplicationInfoSlave(h.config.MasterHost, h.config.MasterPort)
		info += slaveInfo
	} else {
		// Master role (default)
		masterInfo := replication.GetReplicationInfoMaster()
		info += masterInfo
	}

	// Common fields for both roles
	info += "second_repl_offset:-1\n"
	info += "repl_backlog_active:0\n"
	info += "repl_backlog_size:1048576\n"
	info += "repl_backlog_first_byte_offset:0\n"
	info += "repl_backlog_histlen:0"

	return info
}

// getAllInfoSections returns all info sections
func (h *CommandHandler) getAllInfoSections() string {
	// For now, we'll just include the replication section
	// In a full Redis implementation, this would include all sections
	return h.getReplicationInfo()
}

// handleReplconfCommand handles the REPLCONF command
func (h *CommandHandler) handleReplconfCommand(resp protocol.RESP, conn net.Conn) (protocol.RESP, error) {
	if len(resp.Elements) < 2 {
		return protocol.RESP{}, fmt.Errorf("wrong number of arguments for 'replconf' command")
	}

	// Get the subcommand
	subCommand := ""
	if resp.Elements[1].Type == protocol.RESP_BULK_STRING {
		subCommand = strings.ToLower(resp.Elements[1].Str)
	}

	// Extract arguments
	args := make([]string, 0, len(resp.Elements)-2)
	for i := 2; i < len(resp.Elements); i++ {
		if resp.Elements[i].Type == protocol.RESP_BULK_STRING {
			args = append(args, resp.Elements[i].Str)
		}
	}

	// Handle REPLCONF
	err := replication.HandleReplconfCommand(subCommand, args, conn)
	if err != nil {
		return protocol.RESP{}, err
	}

	// Always respond with OK
	return protocol.RESP{Type: protocol.RESP_SIMPLE_STRING, Str: "OK"}, nil
}

// handlePsyncCommand handles the PSYNC command
func (h *CommandHandler) handlePsyncCommand(resp protocol.RESP, conn net.Conn) (protocol.RESP, error) {
	if len(resp.Elements) < 3 {
		return protocol.RESP{}, fmt.Errorf("wrong number of arguments for 'psync' command")
	}

	replID := resp.Elements[1].Str
	offset := resp.Elements[2].Str

	err := replication.HandlePsyncCommand(replID, offset, conn)
	if err != nil {
		return protocol.RESP{}, err
	}

	// Respond with FULLRESYNC
	response := fmt.Sprintf("FULLRESYNC %s 0", replication.GetMasterReplicationID())
	return protocol.RESP{Type: protocol.RESP_SIMPLE_STRING, Str: response}, nil
}

// handleWaitCommand handles the WAIT command
// It returns the number of connected replicas, regardless of the requested count
func (h *CommandHandler) handleWaitCommand(resp protocol.RESP) (protocol.RESP, error) {
	// Check that the command has the correct number of arguments
	if len(resp.Elements) < 3 {
		return protocol.RESP{}, fmt.Errorf("wrong number of arguments for 'wait' command")
	}

	// Parse arguments
	// numReplicas := resp.Elements[1].Str (not used in this implementation)
	// timeout := resp.Elements[2].Str (not used in this implementation)

	// In a full implementation, we would:
	// 1. Block until numReplicas have acknowledged all commands or timeout is reached
	// 2. Return the number of replicas that acknowledged

	// For this stage, we immediately return the current replica count
	replicaCount := replication.GetConnectedReplicaCount()
	return protocol.RESP{Type: protocol.RESP_INTEGER, Num: replicaCount}, nil
}
