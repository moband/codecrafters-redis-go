package replication

import (
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// Connection contexts map
var connectionContexts = struct {
	sync.RWMutex
	m map[net.Conn]*ConnectionContext
}{
	m: make(map[net.Conn]*ConnectionContext),
}

// GetConnectionContext gets the context for a connection
func GetConnectionContext(conn net.Conn) *ConnectionContext {
	connectionContexts.RLock()
	ctx, exists := connectionContexts.m[conn]
	connectionContexts.RUnlock()

	if !exists {
		// Create a new context for this connection
		ctx = &ConnectionContext{
			IsReplica: false,
			Addr:      conn.RemoteAddr().String(),
		}

		connectionContexts.Lock()
		connectionContexts.m[conn] = ctx
		connectionContexts.Unlock()
	}

	return ctx
}

// MarkConnectionAsReplica marks a connection as being a replica
func MarkConnectionAsReplica(conn net.Conn, replicaID string) {
	connectionContexts.Lock()
	defer connectionContexts.Unlock()

	ctx, exists := connectionContexts.m[conn]
	if !exists {
		ctx = &ConnectionContext{
			Addr: conn.RemoteAddr().String(),
			Conn: conn,
		}
		connectionContexts.m[conn] = ctx
	}

	ctx.IsReplica = true
	ctx.ReplicaID = replicaID
	ctx.Conn = conn

	LogInfo("Marked connection %s as replica %s", ctx.Addr, replicaID)
}

// RemoveConnectionContext removes the context for a closed connection
func RemoveConnectionContext(conn net.Conn) {
	connectionContexts.Lock()
	defer connectionContexts.Unlock()

	if ctx, exists := connectionContexts.m[conn]; exists {
		if ctx.IsReplica {
			// In a real implementation, we would clean up the replica state too
			LogInfo("Replica %s disconnected", ctx.ReplicaID)
		}

		delete(connectionContexts.m, conn)
	}
}

// RegisterReplica registers a new replica connection
func RegisterReplica(addr string) string {
	GlobalMasterState.mu.Lock()
	defer GlobalMasterState.mu.Unlock()

	// Create a new replica ID
	replicaID := fmt.Sprintf("replica-%d", GlobalMasterState.nextReplicaID)
	GlobalMasterState.nextReplicaID++

	// Create and store replica info
	GlobalMasterState.replicas[replicaID] = &ReplicaInfo{
		ID:           replicaID,
		Addr:         addr,
		LastPingTime: time.Now(),
	}

	LogInfo("Registered new replica: %s from %s", replicaID, addr)
	return replicaID
}

// UpdateReplicaInfo updates information about a replica
func UpdateReplicaInfo(id string, key string, value string) {
	GlobalMasterState.mu.Lock()
	defer GlobalMasterState.mu.Unlock()

	replica, exists := GlobalMasterState.replicas[id]
	if !exists {
		LogError("Attempted to update unknown replica: %s", id)
		return
	}

	switch key {
	case "port":
		replica.Port = value
		LogDebug("Updated replica %s port to %s", id, value)
	case "flag":
		// Add to flags if not already present
		for _, flag := range replica.Flags {
			if flag == value {
				return // Flag already exists
			}
		}
		replica.Flags = append(replica.Flags, value)
		LogDebug("Added flag %s to replica %s", value, id)
	}
}

// GetConnectedReplicaCount returns the number of connected replicas
func GetConnectedReplicaCount() int {
	GlobalMasterState.mu.RLock()
	defer GlobalMasterState.mu.RUnlock()
	return len(GlobalMasterState.replicas)
}

// GetReplicationInfoMaster returns replication info formatted for INFO command (master perspective)
func GetReplicationInfoMaster() string {
	info := "role:master\n"

	// Get number of connected replicas
	connectedSlaves := GetConnectedReplicaCount()
	info += fmt.Sprintf("connected_slaves:%d\n", connectedSlaves)

	// Add information about each connected replica
	if connectedSlaves > 0 {
		GlobalMasterState.mu.RLock()
		slaveIndex := 0
		for _, replica := range GlobalMasterState.replicas {
			info += fmt.Sprintf("slave%d:ip=%s,port=%s,state=online,offset=0,lag=0\n",
				slaveIndex, replica.Addr, replica.Port)
			slaveIndex++
		}
		GlobalMasterState.mu.RUnlock()
	}

	info += fmt.Sprintf("master_replid:%s\n", GlobalMasterState.masterReplID)
	info += fmt.Sprintf("master_repl_offset:%d\n", GlobalMasterState.masterReplOffset)

	return info
}

// HandleReplconfCommand handles the REPLCONF command from a replica
func HandleReplconfCommand(subCommand string, args []string, conn net.Conn) error {
	LogInfo("Handling REPLCONF %s from replica", subCommand)

	// Get or create connection context
	ctx := GetConnectionContext(conn)

	switch strings.ToLower(subCommand) {
	case "listening-port":
		if len(args) < 1 {
			return fmt.Errorf("missing port argument for REPLCONF listening-port")
		}

		// Get replica's listening port
		port := args[0]
		LogInfo("Replica is listening on port %s", port)

		// Register the replica if it's a new connection
		replicaID := ""
		if !ctx.IsReplica {
			replicaID = RegisterReplica(ctx.Addr)
			MarkConnectionAsReplica(conn, replicaID)
		} else {
			replicaID = ctx.ReplicaID
		}

		// Update the replica's port
		UpdateReplicaInfo(replicaID, "port", port)

	case "capa":
		if len(args) < 1 {
			return fmt.Errorf("missing capability argument for REPLCONF capa")
		}

		// Ensure this connection is marked as a replica
		if !ctx.IsReplica {
			replicaID := RegisterReplica(ctx.Addr)
			MarkConnectionAsReplica(conn, replicaID)
		}

		// Process all capabilities
		for _, capability := range args {
			capability = strings.ToLower(capability)
			LogInfo("Replica %s supports capability: %s", ctx.ReplicaID, capability)

			// Add capability to replica info
			UpdateReplicaInfo(ctx.ReplicaID, "flag", capability)
		}
	}

	return nil
}

// HandlePsyncCommand handles the PSYNC command from a replica
func HandlePsyncCommand(replID string, offset string, conn net.Conn) error {
	LogInfo("Handling PSYNC %s %s from replica", replID, offset)

	// Mark this connection as a replica
	ctx := GetConnectionContext(conn)
	if !ctx.IsReplica {
		// Generate a new replica ID if not already done
		replicaID := RegisterReplica(conn.RemoteAddr().String())
		MarkConnectionAsReplica(conn, replicaID)
	}

	// After responding with FULLRESYNC in the command handler,
	// we'll send the RDB file directly to the replica
	go func() {
		// Small delay to ensure FULLRESYNC response is sent first
		time.Sleep(50 * time.Millisecond)

		// Get the RDB file bytes
		rdbFileBytes := GetEmptyRDBFileBytes()

		// Send the file size followed by the file contents
		// Format: $<length>\r\n<contents>
		prefix := fmt.Sprintf("$%d\r\n", len(rdbFileBytes))

		// Send the prefix
		if _, err := conn.Write([]byte(prefix)); err != nil {
			LogError("Failed to send RDB file size to replica: %v", err)
			return
		}

		// Send the RDB file
		if _, err := conn.Write(rdbFileBytes); err != nil {
			LogError("Failed to send RDB file to replica: %v", err)
			return
		}

		LogInfo("Successfully sent empty RDB file to replica")
	}()

	return nil
}

// GetEmptyRDBFileBytes returns the bytes of an empty RDB file
func GetEmptyRDBFileBytes() []byte {
	// Empty RDB file hex representation (provided in the challenge)
	emptyRDBHex := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

	// Convert hex to bytes
	bytes := make([]byte, hex.DecodedLen(len(emptyRDBHex)))
	n, err := hex.Decode(bytes, []byte(emptyRDBHex))
	if err != nil {
		LogError("Failed to decode RDB hex: %v", err)
		return []byte{}
	}

	return bytes[:n]
}

// GetMasterReplicationID returns the master's replication ID
func GetMasterReplicationID() string {
	GlobalMasterState.mu.RLock()
	defer GlobalMasterState.mu.RUnlock()
	return GlobalMasterState.masterReplID
}

// PropagateCommand propagates a command to all connected replicas
func PropagateCommand(resp protocol.RESP) {
	// Skip propagation if it's not a write command
	if !isWriteCommand(resp) {
		return
	}

	// Lock the master state to get a consistent snapshot of connected replicas
	GlobalMasterState.mu.RLock()
	replicas := make(map[string]*ReplicaInfo, len(GlobalMasterState.replicas))
	for id, replica := range GlobalMasterState.replicas {
		replicas[id] = replica
	}
	GlobalMasterState.mu.RUnlock()

	// Convert the RESP to a raw byte array (as RESP array)
	commandBytes := protocol.BuildRESPCommandFromRESP(resp)

	// Send to each replica asynchronously
	for id := range replicas {
		// Skip replicas that don't have a connection context
		ctx := findConnectionContextByReplicaID(id)
		if ctx == nil || ctx.Conn == nil {
			continue
		}

		// Use the replica's connection to send the command
		conn := ctx.Conn
		go func(replicaID string, conn net.Conn) {
			if _, err := conn.Write([]byte(commandBytes)); err != nil {
				LogError("Failed to propagate command to replica %s: %v", replicaID, err)
			} else {
				LogDebug("Propagated command to replica %s: %s", replicaID, commandBytes)
			}
		}(id, conn)
	}
}

// isWriteCommand determines if a command is a write command that should be propagated
func isWriteCommand(resp protocol.RESP) bool {
	if resp.Type != protocol.RESP_ARRAY || len(resp.Elements) == 0 {
		return false
	}

	commandResp := resp.Elements[0]
	if commandResp.Type != protocol.RESP_BULK_STRING {
		return false
	}

	// List of write commands that should be propagated
	command := strings.ToUpper(commandResp.Str)
	writeCommands := map[string]bool{
		"SET":              true,
		"DEL":              true,
		"SETEX":            true,
		"PSETEX":           true,
		"SETNX":            true,
		"APPEND":           true,
		"INCR":             true,
		"DECR":             true,
		"INCRBY":           true,
		"DECRBY":           true,
		"LPUSH":            true,
		"RPUSH":            true,
		"LPOP":             true,
		"RPOP":             true,
		"LINSERT":          true,
		"LSET":             true,
		"LREM":             true,
		"LTRIM":            true,
		"HDEL":             true,
		"HSET":             true,
		"HSETNX":           true,
		"HINCRBY":          true,
		"SADD":             true,
		"SREM":             true,
		"SMOVE":            true,
		"SPOP":             true,
		"ZADD":             true,
		"ZREM":             true,
		"ZINCRBY":          true,
		"ZREMRANGEBYRANK":  true,
		"ZREMRANGEBYSCORE": true,
		"EXPIRE":           true,
		"EXPIREAT":         true,
		"PEXPIRE":          true,
		"PEXPIREAT":        true,
	}

	return writeCommands[command]
}

// findConnectionContextByReplicaID finds a connection context by replica ID
func findConnectionContextByReplicaID(replicaID string) *ConnectionContext {
	connectionContexts.RLock()
	defer connectionContexts.RUnlock()

	for _, ctx := range connectionContexts.m {
		if ctx.IsReplica && ctx.ReplicaID == replicaID {
			return ctx
		}
	}

	return nil
}
