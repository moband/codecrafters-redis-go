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
		return fmt.Errorf("failed to connect to master: %w", err)
	}

	// Set up connection state
	UpdateReplicationState(true, conn, nil)

	defer func() {
		conn.Close()
		UpdateReplicationState(false, nil, fmt.Errorf("connection closed"))
	}()

	// Create a buffered reader for the connection
	reader := bufio.NewReader(conn)

	// Execute the handshake process
	err = executeHandshake(conn, reader, listenPort)
	if err != nil {
		LogError("Replication handshake failed: %v", err)
		return fmt.Errorf("handshake failed: %w", err)
	}

	LogInfo("Replication handshake completed successfully")

	// In a future stage, we'll handle the RDB file and streaming updates
	// For now, just keep the connection open
	select {} // Block forever
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
		return handleFullResyncResponse(response.Str)
	}

	return fmt.Errorf("unexpected response to PSYNC: %v", response)
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
	// In future implementation, this will save the RDB file
	// For now, just consume the bytes
	buffer := make([]byte, 8192)
	remaining := size

	for remaining > 0 {
		toRead := remaining
		if toRead > int64(len(buffer)) {
			toRead = int64(len(buffer))
		}

		n, err := reader.Read(buffer[:toRead])
		if err != nil {
			return fmt.Errorf("error reading RDB data: %w", err)
		}

		remaining -= int64(n)
	}

	LogInfo("Received %d bytes of RDB data", size)
	return nil
}
