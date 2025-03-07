package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/commands"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/replication"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

// Global variables
var (
	logger         = utils.NewLogger("Main")
	kvStore        = store.NewKeyValueStore()
	cmdConfig      commands.Config
	commandHandler *commands.CommandHandler
)

// ServerConfig stores server configuration options
type ServerConfig struct {
	dir        string
	dbfilename string
	port       string
	role       string
	masterHost string
	masterPort string
}

func main() {
	// Initialize configuration by parsing command line args
	serverConfig := initConfig()

	// Update command configuration from server config
	cmdConfig = commands.Config{
		Role:       serverConfig.role,
		MasterHost: serverConfig.masterHost,
		MasterPort: serverConfig.masterPort,
		Port:       serverConfig.port,
		Dir:        serverConfig.dir,
		DBFilename: serverConfig.dbfilename,
	}

	// Initialize command handler
	commandHandler = commands.NewCommandHandler(kvStore, cmdConfig)

	// Load RDB file if it exists
	if err := loadRDBFile(serverConfig.dir, serverConfig.dbfilename); err != nil {
		logger.Error("Error loading RDB file: %v", err)
	}

	// Start replication if we're a slave/replica
	if serverConfig.role == "slave" {
		logger.Info("Starting Redis server in replica mode (slave of %s:%s)",
			serverConfig.masterHost, serverConfig.masterPort)

		// Start connection to master in a goroutine
		go func() {
			err := replication.ConnectToMaster(serverConfig.masterHost, serverConfig.masterPort, serverConfig.port)
			if err != nil {
				logger.Error("Failed to connect to master: %v", err)
			}
		}()
	} else {
		logger.Info("Starting Redis server in master mode")
	}

	// Start TCP server
	addr := fmt.Sprintf("0.0.0.0:%s", serverConfig.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("Failed to bind to port %s: %v", serverConfig.port, err)
		os.Exit(1)
	}

	logger.Info("Listening on port %s", serverConfig.port)

	// Accept client connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error("Error accepting connection: %v", err)
			continue
		}

		go handleClient(conn)
	}
}

// initConfig initializes the server configuration
func initConfig() *ServerConfig {
	config := &ServerConfig{
		dir:        "./",
		dbfilename: "dump.rdb",
		port:       "6379",
		role:       "master", // Default role is master
	}

	flag.StringVar(&config.dir, "dir", "./", "Directory to store the database files")
	flag.StringVar(&config.dbfilename, "dbfilename", "dump.rdb", "Name of the database file")
	flag.StringVar(&config.port, "port", "6379", "Port to listen on")

	// Replication flags
	var replicaof string
	flag.StringVar(&replicaof, "replicaof", "", "Master host and port (e.g., \"localhost 6379\")")

	flag.Parse()

	// Process the replicaof flag if provided
	if replicaof != "" {
		parts := strings.Fields(replicaof)
		if len(parts) == 2 {
			config.role = "slave"
			config.masterHost = parts[0]
			config.masterPort = parts[1]
		} else {
			fmt.Fprintf(os.Stderr, "Invalid --replicaof value: %s (expected 'host port')\n", replicaof)
		}
	}

	return config
}

// loadRDBFile loads and parses an RDB file
func loadRDBFile(dir, filename string) error {
	rdbPath := filepath.Join(dir, filename)

	// Check if file exists
	if _, err := os.Stat(rdbPath); os.IsNotExist(err) {
		logger.Info("RDB file does not exist, starting with empty database")
		return nil
	}

	logger.Info("Loading RDB file from: %s", rdbPath)

	// Open the RDB file
	file, err := os.Open(rdbPath)
	if err != nil {
		return fmt.Errorf("failed to open RDB file: %w", err)
	}
	defer file.Close()

	// Create RDB parser
	parser := rdb.NewRDBParser(file, kvStore)

	// Parse the RDB file
	if err := parser.Parse(); err != nil {
		return fmt.Errorf("failed to parse RDB file: %w", err)
	}

	logger.Info("Successfully loaded RDB file")
	return nil
}

// handleClient handles a client connection
func handleClient(conn net.Conn) {
	defer func() {
		conn.Close()
		replication.RemoveConnectionContext(conn)
	}()

	logger.Debug("New client connected: %s", conn.RemoteAddr())
	reader := bufio.NewReader(conn)

	for {
		// Parse RESP input
		resp, err := protocol.ParseRESP(reader)
		if err != nil {
			if err != io.EOF {
				logger.Error("Error parsing client request: %v", err)
			}
			break
		}

		// Execute command
		cmdResp, err := commandHandler.ExecuteCommand(resp, conn)
		if err != nil {
			logger.Error("Error executing command: %v", err)
			errorResp := protocol.CreateErrorResp(err.Error())
			if err := protocol.SendRESP(conn, errorResp); err != nil {
				logger.Error("Error sending error response: %v", err)
				break
			}
			continue
		}

		// Send response
		if err := protocol.SendRESP(conn, cmdResp); err != nil {
			logger.Error("Error sending response: %v", err)
			break
		}
	}
}
