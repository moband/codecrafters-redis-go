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
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

var store = rdb.NewKeyValueStore()
var config = NewConfig()
var logger = utils.NewLogger("RDB-Loader")

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
		info += fmt.Sprintf("role:slave\n")
		info += fmt.Sprintf("master_host:%s\n", config.masterHost)
		info += fmt.Sprintf("master_port:%s\n", config.masterPort)
	} else {
		info += "role:master\n"
	}

	// Common fields for both roles master and slave
	info += "connected_slaves:0\n"
	info += "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\n"
	info += "master_repl_offset:0\n"
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

	retryDelay := config.retryDelay
	var conn net.Conn
	var err error

	// Try to connect with retries
	for retry := 0; retry < config.maxRetries; retry++ {
		// Establish TCP connection to master
		conn, err = net.Dial("tcp", masterAddr)
		if err == nil {
			break
		}

		logger.Error("Failed to connect to master (attempt %d/%d): %v",
			retry+1, config.maxRetries, err)

		if retry < config.maxRetries-1 {
			logger.Info("Retrying in %v...", retryDelay)
			time.Sleep(retryDelay)
			// Exponential backoff (up to a point)
			if retryDelay < config.maxReconnectDelay {
				retryDelay *= 2
			}
		}
	}

	if err != nil {
		logger.Error("Failed to connect to master after %d attempts", config.maxRetries)
		return
	}

	defer conn.Close()
	logger.Info("Connected to master, initiating handshake")

	// Send PING command as first part of handshake
	pingCmd := buildRESPCommand("PING")
	_, err = conn.Write([]byte(pingCmd))
	if err != nil {
		logger.Error("Failed to send PING to master: %v", err)
		return
	}

	logger.Info("Sent PING to master")

	// Read the response
	reader := bufio.NewReader(conn)
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

	// In a future stage, we would continue with REPLCONF
	logger.Info("PING-PONG handshake completed")

	// Keep the connection open but don't do anything with it yet
	// This will be expanded in future stages
	select {} // Block forever
}
