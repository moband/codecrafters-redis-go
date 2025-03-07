package replication

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
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
		}
		connectionContexts.m[conn] = ctx
	}

	ctx.IsReplica = true
	ctx.ReplicaID = replicaID

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

	// For this stage, we just return FULLRESYNC with our replication ID and offset
	return nil
}

// GetMasterReplicationID returns the master's replication ID
func GetMasterReplicationID() string {
	GlobalMasterState.mu.RLock()
	defer GlobalMasterState.mu.RUnlock()
	return GlobalMasterState.masterReplID
}
