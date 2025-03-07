package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
)

// Config stores Redis server configuration
type Config struct {
	mu         sync.RWMutex
	dir        string
	dbfilename string
	port       string
	role       string
	masterHost string // Set when role is "slave"
	masterPort string // Set when role is "slave"
}

func (c *Config) Get(param string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	switch param {
	case "dir":
		return c.dir, true
	case "dbfilename":
		return c.dbfilename, true
	case "port":
		return c.port, true
	case "role":
		return c.role, true
	case "masterhost":
		return c.masterHost, true
	case "masterport":
		return c.masterPort, true
	default:
		return "", false
	}
}

func NewConfig() *Config {
	return &Config{
		dir:        "./",
		dbfilename: "dump.rdb",
		port:       "6379",
		role:       "master",
	}
}

func parseCommandLineArgs() {
	flag.StringVar(&config.dir, "dir", "./", "Directory to store the database files")
	flag.StringVar(&config.dbfilename, "dbfilename", "dump.rdb", "Name of the database file")
	flag.StringVar(&config.port, "port", "6379", "Port to listen on")

	var replicaof string
	flag.StringVar(&replicaof, "replicaof", "", "Master host and port (e.g., \"localhost 6379\")")

	flag.Parse()

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
}
