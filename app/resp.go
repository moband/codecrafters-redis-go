package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
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

func createErrorResp(message string) RESP {
	return RESP{Type: RESP_ERROR, Str: fmt.Sprintf("ERR %s", message)}
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

// buildRESPCommand creates a RESP Array command string from given arguments
func buildRESPCommand(args ...string) string {
	// Start with the array length
	result := fmt.Sprintf("*%d\r\n", len(args))

	// Add each argument as a bulk string
	for _, arg := range args {
		result += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}

	return result
}

// formatREPLCONFPort formats a REPLCONF listening-port command
func formatREPLCONFPort(port string) string {
	return buildRESPCommand("REPLCONF", "listening-port", port)
}

// formatREPLCONFCapa formats a REPLCONF capa command
func formatREPLCONFCapa(capabilities ...string) string {
	args := []string{"REPLCONF", "capa"}
	args = append(args, capabilities...)
	return buildRESPCommand(args...)
}

// validateRESPCommand is a debug helper to print the raw bytes of a RESP command
func validateRESPCommand(cmd string) string {
	result := ""
	for i, b := range []byte(cmd) {
		if i > 0 {
			result += " "
		}
		result += fmt.Sprintf("%02X", b)
	}
	return result
}
