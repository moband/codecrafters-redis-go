package main

import (
	"flag"
	"strings"
	"sync"
)

// Config stores Redis server configuration
type Config struct {
	mu         sync.RWMutex
	dir        string
	dbfilename string
	port       string
}

func (c *Config) Get(param string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	switch strings.ToLower(param) {
	case "dir":
		return c.dir, true
	case "dbfilename":
		return c.dbfilename, true
	case "port":
		return c.port, true
	default:
		return "", false
	}

}

func NewConfig() *Config {

	return &Config{
		dir:        "./",
		dbfilename: "dump.rdb",
	}
}

func parseCommandLineArgs() {

	flag.StringVar(&config.dir, "dir", "./", "Directory to store the database files")
	flag.StringVar(&config.dbfilename, "dbfilename", "dump.rdb", "Name of the database file")
	flag.StringVar(&config.port, "port", "6379", "Port to listen on")
	flag.Parse()
}
