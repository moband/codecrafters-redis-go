// Package replication implements Redis replication functionality
//
// This file contains the implementation of the slave (replica) side of replication.
// It handles:
//   1. Connection to a master server
//   2. Replication handshake (PING, REPLCONF, PSYNC)
//   3. RDB file transfer
//   4. Command propagation and execution
//   5. ACK responses for offset tracking
//
// Replication Protocol Flow:
//   1. Slave connects to master
//   2. Handshake sequence:
//      - Slave sends PING, master replies with PONG
//      - Slave sends REPLCONF with capabilities, master replies with OK
//      - Slave sends PSYNC with replication ID and offset, master replies with FULLRESYNC
//   3. Master sends RDB file (snapshot of current state)
//   4. Master propagates write commands to slave
//   5. Master periodically sends REPLCONF GETACK, slave responds with current offset
//
// Offset Tracking:
//   - Slave tracks the number of bytes of commands processed
//   - When master sends REPLCONF GETACK, slave responds with REPLCONF ACK <offset>
//   - This allows master to know how much data the slave has processed

package replication

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

// SlaveState represents the current state of the slave replication process
type SlaveState string

const (
	// StateDisconnected means not connected to a master
	StateDisconnected SlaveState = "disconnected"
	// StateConnecting means attempting to connect to master
	StateConnecting SlaveState = "connecting"
	// StateConnected means connected but handshake not complete
	StateConnected SlaveState = "connected"
	// StateSyncing means handshake complete, receiving initial sync
	StateSyncing SlaveState = "syncing"
	// StateReplicating means fully synchronized and receiving updates
	StateReplicating SlaveState = "replicating"
)

// Global variable to store access to the key-value store
var globalKVStore *store.KeyValueStore

// SetKeyValueStore sets the global key-value store reference
func SetKeyValueStore(kvStore *store.KeyValueStore) {
	globalKVStore = kvStore
}

// getKeyValueStore returns the global key-value store
func getKeyValueStore() (*store.KeyValueStore, error) {
	if globalKVStore == nil {
		return nil, fmt.Errorf("key-value store not initialized")
	}
	return globalKVStore, nil
}

// UpdateReplicationState updates the slave replication state
func UpdateReplicationState(connected bool, conn net.Conn, err error) {
	GlobalReplicationState.mu.Lock()
	defer GlobalReplicationState.mu.Unlock()

	GlobalReplicationState.connected = connected
	GlobalReplicationState.masterConn = conn
	GlobalReplicationState.lastError = err

	if connected {
		GlobalReplicationState.lastPingTime = time.Now()
	}
}

// MarkHandshakeCompleted marks the replication handshake as completed
func MarkHandshakeCompleted() {
	GlobalReplicationState.mu.Lock()
	defer GlobalReplicationState.mu.Unlock()
	GlobalReplicationState.handshakeCompleted = true
}

// SetMasterReplInfo sets the master replication ID and offset
func SetMasterReplInfo(replID string, offset int64) {
	GlobalReplicationState.mu.Lock()
	defer GlobalReplicationState.mu.Unlock()
	GlobalReplicationState.masterReplId = replID
	GlobalReplicationState.masterReplOffset = offset
}

// GetReplicationState returns a copy of the current replication state (thread-safe)
func GetReplicationState() *ReplicationState {
	GlobalReplicationState.mu.RLock()
	defer GlobalReplicationState.mu.RUnlock()

	// Return a copy to avoid race conditions
	return &ReplicationState{
		masterHost:         GlobalReplicationState.masterHost,
		masterPort:         GlobalReplicationState.masterPort,
		masterReplId:       GlobalReplicationState.masterReplId,
		masterReplOffset:   GlobalReplicationState.masterReplOffset,
		handshakeCompleted: GlobalReplicationState.handshakeCompleted,
		connected:          GlobalReplicationState.connected,
		lastPingTime:       GlobalReplicationState.lastPingTime,
	}
}

// GetReplicationInfoSlave returns replication info formatted for INFO command (slave perspective)
func GetReplicationInfoSlave(masterHost, masterPort string) string {
	// Get the current replication state
	state := GetReplicationState()

	info := ""

	// Slave role
	info += fmt.Sprintf("role:slave\n")
	info += fmt.Sprintf("master_host:%s\n", masterHost)
	info += fmt.Sprintf("master_port:%s\n", masterPort)
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

	return info
}

// ConnectToMaster establishes a connection to the master server and performs replication handshake
func ConnectToMaster(masterHost, masterPort, listenPort string) error {
	// Initialize connection to master
	conn, reader, err := initializeMasterConnection(masterHost, masterPort)
	if err != nil {
		return err
	}
	defer closeConnection(conn)

	// Execute the handshake process
	if err := executeHandshake(conn, reader, listenPort); err != nil {
		LogError("Replication handshake failed: %v", err)
		return fmt.Errorf("handshake failed: %w", err)
	}

	LogInfo("Replication handshake completed successfully")

	// Start command processing loop
	return processCommandLoop(conn, reader)
}

// initializeMasterConnection sets up the connection to the master server
func initializeMasterConnection(masterHost, masterPort string) (net.Conn, *bufio.Reader, error) {
	// Update state
	GlobalReplicationState.mu.Lock()
	GlobalReplicationState.masterHost = masterHost
	GlobalReplicationState.masterPort = masterPort
	GlobalReplicationState.mu.Unlock()

	masterAddr := fmt.Sprintf("%s:%s", masterHost, masterPort)
	LogInfo("Connecting to master at %s", masterAddr)

	// Connect to the master server with retries
	conn, err := connectWithRetry(masterAddr, 10, 1*time.Second, 10*time.Second)
	if err != nil {
		UpdateReplicationState(false, nil, err)
		return nil, nil, fmt.Errorf("failed to connect to master: %w", err)
	}

	// Set up connection state
	UpdateReplicationState(true, conn, nil)

	// Create a buffered reader
	reader := bufio.NewReader(conn)

	return conn, reader, nil
}

// closeConnection properly closes the connection and updates the state
func closeConnection(conn net.Conn) {
	if conn != nil {
		conn.Close()
		UpdateReplicationState(false, nil, fmt.Errorf("connection closed"))
	}
}

// processCommandLoop handles the main command processing loop after successful handshake
func processCommandLoop(conn net.Conn, reader *bufio.Reader) error {
	// Set up a dedicated store for processing commands
	kvStore, err := getKeyValueStore()
	if err != nil {
		LogError("Failed to get key-value store: %v", err)
		return fmt.Errorf("failed to get key-value store: %w", err)
	}

	// Mark the handshake as completed to update the replication state
	MarkHandshakeCompleted()

	// Reset bytes counter after handshake
	resetProcessedBytes()

	LogInfo("Starting to process propagated commands from master")

	// Process commands from the master
	for {
		command, commandBytes, cmdName, err := readNextCommand(reader)
		if err != nil {
			if err == io.EOF {
				LogInfo("Master connection closed")
				return fmt.Errorf("master connection terminated")
			}
			LogError("Error parsing command from master: %v", err)
			continue
		}

		// Process the command based on its type
		if isReplconfGetack(command) {
			handleReplconfGetackCommand(conn, commandBytes)
		} else {
			// For all other commands, increment bytes first, then process
			IncrementProcessedBytes(commandBytes)
			processStandardCommand(command, cmdName, kvStore)
		}
	}
}

// readNextCommand reads the next command from the reader
func readNextCommand(reader *bufio.Reader) (protocol.RESP, int64, string, error) {
	// Read the next command
	command, err := protocol.ParseRESP(reader)
	if err != nil {
		return protocol.RESP{}, 0, "", err
	}

	// Calculate command size
	commandBytes := calculateCommandSize(command)

	// Extract command name for logging
	cmdName := extractCommandName(command)

	LogDebug("Received command from master: %s (%d bytes)", cmdName, commandBytes)
	return command, commandBytes, cmdName, nil
}

// extractCommandName gets the name of the command from a RESP object
func extractCommandName(resp protocol.RESP) string {
	if resp.Type == protocol.RESP_ARRAY && len(resp.Elements) > 0 &&
		resp.Elements[0].Type == protocol.RESP_BULK_STRING {
		return strings.ToUpper(resp.Elements[0].Str)
	}
	return "<unknown>"
}

// isReplconfGetack checks if a command is a REPLCONF GETACK command
func isReplconfGetack(command protocol.RESP) bool {
	if command.Type != protocol.RESP_ARRAY || len(command.Elements) < 3 {
		return false
	}

	cmdName := extractCommandName(command)
	return cmdName == "REPLCONF" &&
		command.Elements[1].Type == protocol.RESP_BULK_STRING &&
		strings.ToUpper(command.Elements[1].Str) == "GETACK"
}

// handleReplconfGetackCommand responds to a REPLCONF GETACK command
func handleReplconfGetackCommand(conn net.Conn, commandBytes int64) {
	// Get current offset before including this command
	currentOffset := GetProcessedBytes()
	LogDebug("Responding to REPLCONF GETACK with offset %d", currentOffset)

	// Build and send REPLCONF ACK response
	offsetStr := strconv.FormatInt(currentOffset, 10)
	response := protocol.BuildRESPCommand("REPLCONF", "ACK", offsetStr)

	_, err := conn.Write([]byte(response))
	if err != nil {
		LogError("Failed to send REPLCONF ACK response: %v", err)
	} else {
		LogDebug("Successfully sent REPLCONF ACK %d to master", currentOffset)
	}

	// Update the processed bytes to include this GETACK command
	IncrementProcessedBytes(commandBytes)
}

// processStandardCommand processes a non-GETACK command
func processStandardCommand(command protocol.RESP, cmdName string, kvStore *store.KeyValueStore) {
	if err := processPropagatedCommand(command, kvStore); err != nil {
		LogError("Error processing command %s: %v", cmdName, err)
	}
}

// resetProcessedBytes resets the processed bytes counter to zero
func resetProcessedBytes() {
	GlobalReplicationState.mu.Lock()
	defer GlobalReplicationState.mu.Unlock()
	GlobalReplicationState.processedBytes = 0
	LogDebug("Reset processed bytes counter to 0")
}

// calculateCommandSize calculates the size of a RESP command in bytes
func calculateCommandSize(resp protocol.RESP) int64 {
	// Convert the RESP back to its wire format to calculate its size
	respStr := protocol.BuildRESPCommandFromRESP(resp)
	return int64(len(respStr))
}

// connectWithRetry attempts to connect to the master with exponential backoff
func connectWithRetry(masterAddr string, maxRetries int, initialRetryDelay, maxRetryDelay time.Duration) (net.Conn, error) {
	retryDelay := initialRetryDelay

	var conn net.Conn
	var err error

	// Try to connect with retries
	for retry := 0; retry < maxRetries; retry++ {
		// Establish TCP connection to master
		conn, err = net.Dial("tcp", masterAddr)
		if err == nil {
			LogInfo("Connected to master")
			return conn, nil
		}

		LogError("Failed to connect to master (attempt %d/%d): %v",
			retry+1, maxRetries, err)

		if retry < maxRetries-1 {
			LogInfo("Retrying in %v...", retryDelay)
			time.Sleep(retryDelay)
			// Exponential backoff (up to a point)
			if retryDelay < maxRetryDelay {
				retryDelay *= 2
			}
		}
	}

	return nil, fmt.Errorf("failed to connect after %d attempts", maxRetries)
}

// executeHandshake performs the three-part replication handshake
func executeHandshake(conn net.Conn, reader *bufio.Reader, listenPort string) error {
	// Step 1: PING-PONG handshake
	if err := executePingHandshake(conn, reader); err != nil {
		return fmt.Errorf("PING handshake failed: %w", err)
	}

	// Step 2: REPLCONF handshake
	if err := executeReplconfHandshake(conn, reader, listenPort); err != nil {
		return fmt.Errorf("REPLCONF handshake failed: %w", err)
	}

	// Step 3: PSYNC handshake
	if err := executePsyncHandshake(conn, reader); err != nil {
		return fmt.Errorf("PSYNC handshake failed: %w", err)
	}

	// Mark handshake as completed
	MarkHandshakeCompleted()
	return nil
}

// executePingHandshake handles the PING-PONG part of the handshake
func executePingHandshake(conn net.Conn, reader *bufio.Reader) error {
	LogInfo("Step 1: Sending PING to master")

	// Send PING command
	pingCmd := protocol.BuildRESPCommand("PING")
	LogDebug("PING command: %s", protocol.ValidateRESPCommand(pingCmd))

	_, err := conn.Write([]byte(pingCmd))
	if err != nil {
		return fmt.Errorf("failed to send PING: %w", err)
	}

	// Read the PING response
	response, err := protocol.ParseRESP(reader)
	if err != nil {
		return fmt.Errorf("failed to read PING response: %w", err)
	}

	// Check if the response is PONG
	if (response.Type == protocol.RESP_SIMPLE_STRING && response.Str == "PONG") ||
		(response.Type == protocol.RESP_BULK_STRING && response.Str == "PONG") {
		LogInfo("Received PONG from master, handshake step 1 complete")
		return nil
	}

	return fmt.Errorf("unexpected response to PING: %v", response)
}

// executeReplconfHandshake handles the REPLCONF part of the handshake
func executeReplconfHandshake(conn net.Conn, reader *bufio.Reader, listenPort string) error {
	// Step 2a: Send REPLCONF listening-port
	LogInfo("Step 2a: Sending REPLCONF listening-port to master")
	replconfPortCmd := protocol.FormatREPLCONFPort(listenPort)
	LogDebug("REPLCONF listening-port command: %s", protocol.ValidateRESPCommand(replconfPortCmd))

	_, err := conn.Write([]byte(replconfPortCmd))
	if err != nil {
		return fmt.Errorf("failed to send REPLCONF listening-port: %w", err)
	}

	// Read the response
	response, err := protocol.ParseRESP(reader)
	if err != nil {
		return fmt.Errorf("failed to read REPLCONF listening-port response: %w", err)
	}

	// Check for OK response
	if response.Type != protocol.RESP_SIMPLE_STRING || response.Str != "OK" {
		return fmt.Errorf("unexpected response to REPLCONF listening-port: %v", response)
	}

	LogInfo("Received OK from master, handshake step 2a complete")

	// Step 2b: Send REPLCONF capa
	LogInfo("Step 2b: Sending REPLCONF capa to master with capabilities: eof, psync2")
	replconfCapaCmd := protocol.FormatREPLCONFCapa("eof", "psync2")
	LogDebug("REPLCONF capa command: %s", protocol.ValidateRESPCommand(replconfCapaCmd))

	_, err = conn.Write([]byte(replconfCapaCmd))
	if err != nil {
		return fmt.Errorf("failed to send REPLCONF capa: %w", err)
	}

	// Read the response
	response, err = protocol.ParseRESP(reader)
	if err != nil {
		return fmt.Errorf("failed to read REPLCONF capa response: %w", err)
	}

	// Check for OK response
	if response.Type != protocol.RESP_SIMPLE_STRING || response.Str != "OK" {
		return fmt.Errorf("unexpected response to REPLCONF capa: %v", response)
	}

	LogInfo("Received OK from master, handshake step 2b complete")
	return nil
}

// executePsyncHandshake handles the PSYNC part of the handshake
func executePsyncHandshake(conn net.Conn, reader *bufio.Reader) error {
	// Step 3: Send PSYNC command
	LogInfo("Step 3: Sending PSYNC ? -1 to master")
	psyncCmd := protocol.FormatPSYNC("?", "-1")
	LogDebug("PSYNC command: %s", protocol.ValidateRESPCommand(psyncCmd))

	_, err := conn.Write([]byte(psyncCmd))
	if err != nil {
		return fmt.Errorf("failed to send PSYNC: %w", err)
	}

	// Read the response
	response, err := protocol.ParseRESP(reader)
	if err != nil {
		return fmt.Errorf("failed to read PSYNC response: %w", err)
	}

	// Check for FULLRESYNC response
	if response.Type == protocol.RESP_SIMPLE_STRING && strings.HasPrefix(response.Str, "FULLRESYNC") {
		LogInfo("Received FULLRESYNC response from master")

		// Process the FULLRESYNC response
		err = handleFullResyncResponse(response.Str)
		if err != nil {
			return fmt.Errorf("failed to handle FULLRESYNC response: %w", err)
		}

		// After handling FULLRESYNC, read the RDB file
		err = receiveRDBFile(reader)
		if err != nil {
			return fmt.Errorf("failed to receive RDB file: %w", err)
		}

		return nil
	}

	return fmt.Errorf("unexpected response to PSYNC: %v", response)
}

// receiveRDBFile receives the RDB file from the master
func receiveRDBFile(reader *bufio.Reader) error {
	// First, read the RDB file size line: $<length>\r\n
	sizeData, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read RDB file size: %w", err)
	}

	if !strings.HasPrefix(sizeData, "$") {
		return fmt.Errorf("invalid RDB file size format (expected $ prefix): %s", sizeData)
	}

	// Parse the size (remove $ and \r\n)
	sizeStr := strings.TrimSuffix(sizeData[1:], "\r\n")
	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid RDB file size: %w", err)
	}

	LogInfo("Reading RDB file of size %d bytes", size)

	// Read the RDB file content
	if err := ReadRDBFile(reader, size); err != nil {
		return fmt.Errorf("failed to read RDB file: %w", err)
	}

	LogInfo("Successfully read RDB file")
	return nil
}

// handleFullResyncResponse processes the FULLRESYNC response from the master
func handleFullResyncResponse(response string) error {
	// Parse the replication ID and offset from the response
	// Format: FULLRESYNC <replid> <offset>
	parts := strings.Split(response, " ")
	if len(parts) >= 3 {
		replID := parts[1]
		offsetStr := parts[2]
		LogInfo("Received FULLRESYNC from master: replID=%s, offset=%s", replID, offsetStr)

		// Parse offset
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid offset in FULLRESYNC response: %w", err)
		}

		// Update replication state
		SetMasterReplInfo(replID, offset)
		return nil
	}

	return fmt.Errorf("malformed FULLRESYNC response: %s", response)
}

// ReadRDBFile reads the RDB file sent by the master after a FULLRESYNC
func ReadRDBFile(reader io.Reader, size int64) error {
	// In future implementation, this will parse and load the RDB file
	// For now, just consume the bytes

	// Use a reasonable buffer size for reading chunks of data
	const bufferSize = 4096
	buffer := make([]byte, bufferSize)
	remaining := size

	LogInfo("Starting to read RDB file data (size: %d bytes)", size)

	// Read the file in chunks until we've consumed all bytes
	for remaining > 0 {
		toRead := remaining
		if toRead > int64(len(buffer)) {
			toRead = int64(len(buffer))
		}

		n, err := io.ReadFull(reader, buffer[:toRead])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				remaining -= int64(n)
				LogWarning("Received EOF while reading RDB data (read: %d, remaining: %d)", n, remaining)
				if remaining > 0 {
					return fmt.Errorf("incomplete RDB file: %d bytes remaining", remaining)
				}
				break
			}
			return fmt.Errorf("error reading RDB data: %w", err)
		}

		remaining -= int64(n)
		if remaining%int64(bufferSize) == 0 || remaining < int64(bufferSize) {
			LogDebug("Read %d bytes of RDB data, %d remaining", n, remaining)
		}
	}

	LogInfo("Successfully read all %d bytes of RDB data", size)
	return nil
}

// processPropagatedCommand executes a command received from the master
// without sending a response back
func processPropagatedCommand(resp protocol.RESP, kvStore *store.KeyValueStore) error {
	// Validate command format
	if err := validateCommandFormat(resp); err != nil {
		return err
	}

	command := strings.ToUpper(resp.Elements[0].Str)
	LogDebug("Processing propagated command: %s with %d arguments", command, len(resp.Elements)-1)

	// Process different types of commands
	switch command {
	case "REPLCONF":
		// REPLCONF GETACK is handled separately in the main loop
		return nil

	case "PING":
		LogDebug("Processing PING command from master (silently)")
		return nil

	case "SET":
		return handleSetCommand(resp, kvStore)

	case "DEL":
		return handleDelCommand(resp, kvStore)

	// Add more command handlers as needed (INCR, LPUSH, etc.)

	default:
		// For this challenge, we'll handle unknown commands gracefully
		LogDebug("Unknown command received from master: %s (ignoring)", command)
		return nil // Don't return error for unknown commands
	}
}

// validateCommandFormat validates that a command has the correct RESP format
func validateCommandFormat(resp protocol.RESP) error {
	if resp.Type != protocol.RESP_ARRAY {
		return fmt.Errorf("command must be an array")
	}

	if len(resp.Elements) == 0 {
		return fmt.Errorf("command array cannot be empty")
	}

	if resp.Elements[0].Type != protocol.RESP_BULK_STRING {
		return fmt.Errorf("command name must be a bulk string")
	}

	return nil
}

// handleSetCommand processes a SET command from the master
func handleSetCommand(resp protocol.RESP, kvStore *store.KeyValueStore) error {
	// Validate SET command has at least key and value arguments
	if len(resp.Elements) < 3 {
		return fmt.Errorf("wrong number of arguments for 'set' command (minimum 2 required)")
	}

	key := resp.Elements[1].Str
	value := resp.Elements[2].Str
	expiry := parseSetExpiry(resp.Elements[3:])

	// Store key-value pair with optional expiry
	kvStore.Set(key, value, expiry)
	LogDebug("Successfully processed SET %s %s (expiry: %v)", key, value, expiry)
	return nil
}

// parseSetExpiry extracts expiry time from SET command options if present
func parseSetExpiry(options []protocol.RESP) time.Duration {
	expiry := time.Duration(0)

	// Check for optional PX argument (expiry in milliseconds)
	for i := 0; i < len(options)-1; i++ {
		if options[i].Type == protocol.RESP_BULK_STRING {
			option := strings.ToUpper(options[i].Str)
			if option == "PX" && i+1 < len(options) && options[i+1].Type == protocol.RESP_BULK_STRING {
				if ms, err := strconv.Atoi(options[i+1].Str); err == nil {
					expiry = time.Duration(ms) * time.Millisecond
					break
				}
			}
		}
	}

	return expiry
}

// handleDelCommand processes a DEL command from the master
func handleDelCommand(resp protocol.RESP, kvStore *store.KeyValueStore) error {
	// Validate DEL command has at least one key argument
	if len(resp.Elements) < 2 {
		return fmt.Errorf("wrong number of arguments for 'del' command (minimum 1 required)")
	}

	// Extract keys to delete
	keys := make([]string, len(resp.Elements)-1)
	for i := 1; i < len(resp.Elements); i++ {
		keys[i-1] = resp.Elements[i].Str
	}

	// In a complete implementation, we would delete the keys here
	// For the challenge, we just log the deletion
	LogDebug("Would delete keys: %v", keys)
	return nil
}

// IncrementProcessedBytes adds the given number of bytes to the processed bytes count
func IncrementProcessedBytes(bytes int64) {
	GlobalReplicationState.mu.Lock()
	defer GlobalReplicationState.mu.Unlock()
	GlobalReplicationState.processedBytes += bytes
	LogDebug("Incremented processed bytes by %d to %d", bytes, GlobalReplicationState.processedBytes)
}

// GetProcessedBytes returns the current processed bytes count
func GetProcessedBytes() int64 {
	GlobalReplicationState.mu.RLock()
	defer GlobalReplicationState.mu.RUnlock()
	return GlobalReplicationState.processedBytes
}
