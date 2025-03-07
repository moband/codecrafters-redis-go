package replication

import (
	"net"
	"sync"
	"time"
)

// ReplicationState tracks the state of replication for a slave
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

// GlobalReplicationState is the global replication state for a slave
var GlobalReplicationState = &ReplicationState{
	masterReplId: "0000000000000000000000000000000000000000", // Initial replication ID
}

// ReplicaInfo stores information about a connected replica
type ReplicaInfo struct {
	ID           string    // Unique identifier for this replica
	Addr         string    // Address of the replica
	Port         string    // Port the replica is listening on
	Flags        []string  // Capabilities of the replica
	LastPingTime time.Time // Time of last activity
}

// MasterState tracks the state of the master and its connected replicas
type MasterState struct {
	mu               sync.RWMutex
	replicas         map[string]*ReplicaInfo // Map of replica ID to replica info
	nextReplicaID    int                     // Next replica ID to assign
	masterReplID     string                  // Master replication ID
	masterReplOffset int64                   // Master replication offset
}

// GlobalMasterState is the global state for a master
var GlobalMasterState = &MasterState{
	replicas:         make(map[string]*ReplicaInfo),
	masterReplID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", // Initial replication ID
	masterReplOffset: 0,
}

// ConnectionContext stores context for a client connection
type ConnectionContext struct {
	IsReplica bool     // Whether this connection is a replica
	ReplicaID string   // ID of the replica (if IsReplica is true)
	Addr      string   // Remote address of the connection
	Conn      net.Conn // The connection object
}
