package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
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

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

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
