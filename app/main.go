package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

var store = rdb.NewKeyValueStore()
var config = NewConfig()
var logger = utils.NewLogger("RDB-Loader")

// ReplicationState tracks the state of replication
type ReplicationState struct {
	mu                 sync.RWMutex
	masterConn         net.Conn
	masterHost         string
	masterPort         string
	masterReplId       string
	masterReplOffset   int64
	handshakeCompleted bool
	connected          bool
	lastError          error
	lastPingTime       time.Time
}

// Global replication state
var replState = &ReplicationState{
	masterReplId: "0000000000000000000000000000000000000000", // Initial replication ID
}

// updateReplicationState updates the replication state
func updateReplicationState(connected bool, conn net.Conn, err error) {
	replState.mu.Lock()
	defer replState.mu.Unlock()

	replState.connected = connected
	replState.masterConn = conn
	replState.lastError = err

	if connected {
		replState.masterHost = config.masterHost
		replState.masterPort = config.masterPort
	}
}

// markHandshakeCompleted marks the replication handshake as completed
func markHandshakeCompleted() {
	replState.mu.Lock()
	defer replState.mu.Unlock()
	replState.handshakeCompleted = true
}

// getReplicationState returns a copy of the current replication state (thread-safe)
func getReplicationState() *ReplicationState {
	replState.mu.RLock()
	defer replState.mu.RUnlock()

	// Return a copy to avoid race conditions
	return &ReplicationState{
		masterHost:         replState.masterHost,
		masterPort:         replState.masterPort,
		masterReplId:       replState.masterReplId,
		masterReplOffset:   replState.masterReplOffset,
		handshakeCompleted: replState.handshakeCompleted,
		connected:          replState.connected,
		lastPingTime:       replState.lastPingTime,
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	parseCommandLineArgs()

	if err := loadRDBFile(); err != nil {
		fmt.Printf("Error loading RDB file: %v\n", err)
	}

	// Print server role
	if config.role == "slave" {
		fmt.Printf("Starting Redis server in replica mode (slave of %s:%s)\n",
			config.masterHost, config.masterPort)

		// Start connection to master in a goroutine
		go connectToMaster()
	} else {
		fmt.Println("Starting Redis server in master mode")
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", config.port))
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to bind to port %s", config.port))
		os.Exit(1)
	}
	logger.Info(fmt.Sprintf("Listening on port %s", config.port))

	for {
		conn, err := l.Accept()
		if err != nil {
			logger.Error(fmt.Sprintf("Error accepting connection: %v", err))
			continue
		}

		go handleClient(conn)
	}
}

// loadRDBFile loads and parses an RDB file
func loadRDBFile() error {

	rdbPath := filepath.Join(config.dir, config.dbfilename)

	if _, err := os.Stat(rdbPath); os.IsNotExist(err) {
		logger.Info("RDB file does not exist, starting with empty database")
		return nil // Return nil to indicate no error if the file doesn't exist
	}

	logger.Info("Loading RDB file from: %s", rdbPath)
	file, err := os.Open(rdbPath)
	if err != nil {
		return fmt.Errorf("failed to open RDB file: %v", err)
	}
	defer file.Close()

	parser := rdb.NewRDBParser(file, store)
	err = parser.Parse()
	if err != nil {
		logger.Error("Failed to parse RDB file: %v", err)
		return err
	}

	logger.Info("Successfully loaded RDB file")
	return nil
}

func handleClient(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {

		resp, err := parseRESP(reader)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error parsing RESP: ", err.Error())
			}
			break
		}

		response, err := executeCommand(resp)
		if err != nil {
			fmt.Println("Error executing command: ", err.Error())
			errorResp := createErrorResp(err.Error())
			sendRESP(conn, errorResp)
			continue
		}

		err = sendRESP(conn, response)
		if err != nil {
			fmt.Println("Error sending response: ", err.Error())
			break
		}
	}
}

func executeCommand(resp RESP) (RESP, error) {

	if resp.Type != RESP_ARRAY || len(resp.Elements) == 0 {
		return RESP{}, fmt.Errorf("invalid command format")
	}

	commandResp := resp.Elements[0]
	if commandResp.Type != RESP_BULK_STRING {
		return RESP{}, fmt.Errorf("command must be a bulk string")
	}

	command := strings.ToUpper(commandResp.Str)

	switch command {
	case "PING":
		return RESP{Type: RESP_SIMPLE_STRING, Str: "PONG"}, nil

	case "ECHO":
		if len(resp.Elements) < 2 {
			return RESP{}, fmt.Errorf("wrong number of arguments for 'echo' command")
		}

		return resp.Elements[1], nil

	case "SET":
		if len(resp.Elements) < 3 {
			return RESP{}, fmt.Errorf("wrong number of arguments for 'set' command")
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
					return RESP{}, fmt.Errorf("invalid expire time in 'set' command")
				}
				expiry = time.Duration(ms) * time.Millisecond
				break
			}
		}

		store.Set(key, value, expiry)

		return RESP{Type: RESP_SIMPLE_STRING, Str: "OK"}, nil

	case "GET":
		if len(resp.Elements) < 2 {
			return RESP{}, fmt.Errorf("wrong number of arguments for 'get' command")
		}

		key := resp.Elements[1].Str

		value, exists := store.Get(key)

		if !exists {
			return RESP{Type: RESP_BULK_STRING, Str: "", Num: -1}, nil
		}

		return RESP{Type: RESP_BULK_STRING, Str: value}, nil

	case "CONFIG":
		if len(resp.Elements) < 3 {
			return RESP{}, fmt.Errorf("wrong number of arguments for 'config' command")
		}

		subcommand := strings.ToUpper(resp.Elements[1].Str)

		switch subcommand {
		case "GET":
			param := resp.Elements[2].Str
			value, exists := config.Get(param)
			if !exists {
				return RESP{Type: RESP_ARRAY, Elements: []RESP{}}, nil
			}

			return RESP{
				Type: RESP_ARRAY,
				Elements: []RESP{
					{Type: RESP_BULK_STRING, Str: param},
					{Type: RESP_BULK_STRING, Str: value},
				},
			}, nil
		}

		return RESP{}, fmt.Errorf("unknown CONFIG subcommand '%s'", subcommand)

	case "KEYS":
		if len(resp.Elements) < 2 {
			return RESP{}, fmt.Errorf("wrong number of arguments for 'keys' command")
		}

		pattern := resp.Elements[1].Str

		if pattern != "*" {
			return RESP{}, fmt.Errorf("only '*' pattern is supported for 'keys' command")
		}

		keys := store.GetAllKeys()
		elements := make([]RESP, len(keys))
		for i, key := range keys {
			elements[i] = RESP{Type: RESP_BULK_STRING, Str: key}
		}

		return RESP{Type: RESP_ARRAY, Elements: elements}, nil

	case "INFO":
		section := ""
		if len(resp.Elements) > 1 {
			section = strings.ToLower(resp.Elements[1].Str)
		}

		var info string
		if section == "" {
			info = getAllInfoSections()
		} else if section == "replication" {
			info = getReplicationInfo()
		} else {
			info = fmt.Sprintf("# %s\n", strings.Title(section))
		}

		return RESP{Type: RESP_BULK_STRING, Str: info}, nil

	default:
		return RESP{}, fmt.Errorf("unknown command '%s'", commandResp.Str)
	}
}

// getReplicationInfo returns information about replication
func getReplicationInfo() string {
	info := "# Replication\n"

	if config.role == "slave" {
		// Get the current replication state
		state := getReplicationState()

		// Slave role
		info += fmt.Sprintf("role:slave\n")
		info += fmt.Sprintf("master_host:%s\n", config.masterHost)
		info += fmt.Sprintf("master_port:%s\n", config.masterPort)
		info += fmt.Sprintf("master_link_status:%s\n",
			map[bool]string{true: "up", false: "down"}[state.connected])

		// Include additional details about the handshake
		if state.handshakeCompleted {
			info += fmt.Sprintf("master_sync_in_progress:0\n")
		} else {
			info += fmt.Sprintf("master_sync_in_progress:1\n")
		}

		// Include replication ID and offset
		info += fmt.Sprintf("master_replid:%s\n", state.masterReplId)
		info += fmt.Sprintf("master_repl_offset:%d\n", state.masterReplOffset)
	} else {
		// Master role (default)
		info += "role:master\n"
		info += "connected_slaves:0\n"
		info += "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\n"
		info += "master_repl_offset:0\n"
	}

	// Common fields for both roles
	info += "second_repl_offset:-1\n"
	info += "repl_backlog_active:0\n"
	info += "repl_backlog_size:1048576\n"
	info += "repl_backlog_first_byte_offset:0\n"
	info += "repl_backlog_histlen:0"

	return info
}

// Returns all info sections for the INFO command
func getAllInfoSections() string {

	return getReplicationInfo()
}

// connectToMaster establishes a connection to the master server and begins replication
func connectToMaster() {
	if config.role != "slave" {
		return
	}

	logger := utils.NewLogger("Replication")
	masterAddr := fmt.Sprintf("%s:%s", config.masterHost, config.masterPort)
	logger.Info("Connecting to master at %s", masterAddr)

	maxRetries := config.maxRetries
	retryDelay := config.retryDelay

	var conn net.Conn
	var err error

	// Try to connect with retries
	for retry := 0; retry < maxRetries; retry++ {
		// Establish TCP connection to master
		conn, err = net.Dial("tcp", masterAddr)
		if err == nil {
			break
		}

		logger.Error("Failed to connect to master (attempt %d/%d): %v",
			retry+1, maxRetries, err)

		if retry < maxRetries-1 {
			logger.Info("Retrying in %v...", retryDelay)
			time.Sleep(retryDelay)
			// Exponential backoff (up to a point)
			if retryDelay < config.maxReconnectDelay {
				retryDelay *= 2
			}
		}
	}

	// If we couldn't connect after all retries
	if err != nil {
		logger.Error("Failed to connect to master after %d attempts", maxRetries)
		updateReplicationState(false, nil, err)
		return
	}

	// Update replication state with the new connection
	updateReplicationState(true, conn, nil)

	defer func() {
		conn.Close()
		updateReplicationState(false, nil, fmt.Errorf("connection closed"))
	}()

	logger.Info("Connected to master, initiating handshake")

	// Create a buffered reader for the connection
	reader := bufio.NewReader(conn)

	// Step 1: Send PING command
	logger.Info("Step 1: Sending PING to master")
	pingCmd := buildRESPCommand("PING")
	logger.Debug("PING command: %s", validateRESPCommand(pingCmd))
	_, err = conn.Write([]byte(pingCmd))
	if err != nil {
		logger.Error("Failed to send PING to master: %v", err)
		return
	}

	// Read the PING response
	response, err := parseRESP(reader)
	if err != nil {
		logger.Error("Failed to read PING response: %v", err)
		return
	}

	// Check if the response is PONG (or +PONG)
	if (response.Type == RESP_SIMPLE_STRING && response.Str == "PONG") ||
		(response.Type == RESP_BULK_STRING && response.Str == "PONG") {
		logger.Info("Received PONG from master, handshake step 1 complete")
	} else {
		logger.Error("Unexpected response to PING: %v", response)
		return
	}

	// Step 2a: Send REPLCONF listening-port
	logger.Info("Step 2a: Sending REPLCONF listening-port to master")
	replconfPortCmd := formatREPLCONFPort(config.port)
	logger.Debug("REPLCONF listening-port command: %s", validateRESPCommand(replconfPortCmd))
	_, err = conn.Write([]byte(replconfPortCmd))
	if err != nil {
		logger.Error("Failed to send REPLCONF listening-port: %v", err)
		return
	}

	// Read the REPLCONF listening-port response
	response, err = parseRESP(reader)
	if err != nil {
		logger.Error("Failed to read REPLCONF listening-port response: %v", err)
		return
	}

	// Check if the response is OK
	if response.Type == RESP_SIMPLE_STRING && response.Str == "OK" {
		logger.Info("Received OK from master, handshake step 2a complete")
	} else {
		logger.Error("Unexpected response to REPLCONF listening-port: %v", response)
		return
	}

	// Step 2b: Send REPLCONF capa psync2
	logger.Info("Step 2b: Sending REPLCONF capa psync2 to master")
	replconfCapaCmd := formatREPLCONFCapa("psync2")
	logger.Debug("REPLCONF capa command: %s", validateRESPCommand(replconfCapaCmd))
	_, err = conn.Write([]byte(replconfCapaCmd))
	if err != nil {
		logger.Error("Failed to send REPLCONF capa psync2: %v", err)
		return
	}

	// Read the REPLCONF capa response
	response, err = parseRESP(reader)
	if err != nil {
		logger.Error("Failed to read REPLCONF capa response: %v", err)
		return
	}

	// Check if the response is OK
	if response.Type == RESP_SIMPLE_STRING && response.Str == "OK" {
		logger.Info("Received OK from master, handshake step 2b complete")
	} else {
		logger.Error("Unexpected response to REPLCONF capa: %v", response)
		return
	}

	// Mark handshake as completed (steps 1 and 2)
	markHandshakeCompleted()
	logger.Info("Replication handshake steps 1 and 2 completed successfully")

	// In the next stage, we'll implement PSYNC (step 3)
	// For now, keep the connection open
	select {} // Block forever
}
