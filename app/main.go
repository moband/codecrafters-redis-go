package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RESP data types
const (
	RESP_SIMPLE_STRING = '+'
	RESP_ERROR         = '-'
	RESP_INTEGER       = ':'
	RESP_BULK_STRING   = '$'
	RESP_ARRAY         = '*'
)

// RESP represents a Redis protocol value
type RESP struct {
	Type     byte
	Str      string
	Num      int
	Elements []RESP
}

// Define a KeyValueStore to store Redis data
type KeyValueStore struct {
	mu   sync.RWMutex
	data map[string]valueWithExpiry
}

// valueWithExpiry holds a value and its expiration time
type valueWithExpiry struct {
	value    string
	expireAt time.Time // Zero time means no expiration
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		data: make(map[string]valueWithExpiry),
	}
}

func (kv *KeyValueStore) Set(key, value string, expiry time.Duration) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var expireAt time.Time
	if expiry > 0 {
		expireAt = time.Now().Add(expiry)
	}

	kv.data[key] = valueWithExpiry{
		value:    value,
		expireAt: expireAt,
	}
}

func (kv *KeyValueStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	entry, exists := kv.data[key]
	if !exists {
		return "", false
	}

	if !entry.expireAt.IsZero() && time.Now().After(entry.expireAt) {
		go func() {
			kv.mu.Lock()
			delete(kv.data, key)
			kv.mu.Unlock()
		}()
		return "", false
	}

	return entry.value, true
}

// Config stores Redis server configuration
type Config struct {
	mu         sync.RWMutex
	dir        string
	dbfilename string
}

func (c *Config) Get(param string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	switch strings.ToLower(param) {
	case "dir":
		return c.dir, true
	case "dbfilename":
		return c.dbfilename, true
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

var store = NewKeyValueStore()
var config = NewConfig()

func parseCommandLineArgs() {

	flag.StringVar(&config.dir, "dir", "./", "Directory to store the database files")
	flag.StringVar(&config.dbfilename, "dbfilename", "dump.rdb", "Name of the database file")
	flag.Parse()
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	parseCommandLineArgs()

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleClient(conn)
	}
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

func parseRESP(reader *bufio.Reader) (RESP, error) {

	respType, err := reader.ReadByte()
	if err != nil {
		return RESP{}, err
	}

	switch respType {
	case RESP_SIMPLE_STRING:
		str, err := readLine(reader)
		if err != nil {
			return RESP{}, err
		}
		return RESP{Type: RESP_SIMPLE_STRING, Str: str}, nil

	case RESP_ERROR:
		str, err := readLine(reader)
		if err != nil {
			return RESP{}, err
		}
		return RESP{Type: RESP_ERROR, Str: str}, nil

	case RESP_INTEGER:
		str, err := readLine(reader)
		if err != nil {
			return RESP{}, err
		}
		num, err := strconv.Atoi(str)
		if err != nil {
			return RESP{}, err
		}
		return RESP{Type: RESP_INTEGER, Num: num}, nil

	case RESP_BULK_STRING:
		length, err := readInteger(reader)
		if err != nil {
			return RESP{}, err
		}

		if length == -1 {
			return RESP{Type: RESP_BULK_STRING, Str: ""}, nil
		}

		str := make([]byte, length)
		_, err = io.ReadFull(reader, str)
		if err != nil {
			return RESP{}, err
		}

		_, err = reader.ReadByte() // \r
		if err != nil {
			return RESP{}, err
		}
		_, err = reader.ReadByte() // \n
		if err != nil {
			return RESP{}, err
		}

		return RESP{Type: RESP_BULK_STRING, Str: string(str)}, nil

	case RESP_ARRAY:
		count, err := readInteger(reader)
		if err != nil {
			return RESP{}, err
		}

		if count == -1 {
			return RESP{Type: RESP_ARRAY, Elements: nil}, nil
		}

		elements := make([]RESP, count)
		for i := 0; i < count; i++ {
			element, err := parseRESP(reader)
			if err != nil {
				return RESP{}, err
			}
			elements[i] = element
		}

		return RESP{Type: RESP_ARRAY, Elements: elements}, nil

	default:

		line, err := reader.ReadString('\n')
		if err != nil {
			return RESP{}, err
		}
		cmd := strings.TrimSpace(line)

		return RESP{
			Type: RESP_ARRAY,
			Elements: []RESP{
				{Type: RESP_BULK_STRING, Str: cmd},
			},
		}, nil
	}
}

func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}

func readInteger(reader *bufio.Reader) (int, error) {
	line, err := readLine(reader)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(line)
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

	default:
		return RESP{}, fmt.Errorf("unknown command '%s'", commandResp.Str)
	}
}

func sendRESP(conn net.Conn, resp RESP) error {
	switch resp.Type {
	case RESP_SIMPLE_STRING:
		_, err := conn.Write([]byte(fmt.Sprintf("+%s\r\n", resp.Str)))
		return err

	case RESP_ERROR:
		_, err := conn.Write([]byte(fmt.Sprintf("-%s\r\n", resp.Str)))
		return err

	case RESP_INTEGER:
		_, err := conn.Write([]byte(fmt.Sprintf(":%d\r\n", resp.Num)))
		return err

	case RESP_BULK_STRING:
		if resp.Num == -1 {
			_, err := conn.Write([]byte("$-1\r\n"))
			return err
		}

		_, err := conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(resp.Str), resp.Str)))
		return err

	case RESP_ARRAY:
		_, err := conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(resp.Elements))))
		if err != nil {
			return err
		}

		for _, element := range resp.Elements {
			err = sendRESP(conn, element)
			if err != nil {
				return err
			}
		}

		return nil

	default:
		return fmt.Errorf("unknown RESP type: %c", resp.Type)
	}
}

func createErrorResp(message string) RESP {
	return RESP{Type: RESP_ERROR, Str: fmt.Sprintf("ERR %s", message)}
}
