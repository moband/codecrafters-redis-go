package protocol

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

// ParseRESP parses a RESP message from a reader
func ParseRESP(reader *bufio.Reader) (RESP, error) {
	respType, err := reader.ReadByte()
	if err != nil {
		return RESP{}, err
	}

	switch respType {
	case RESP_SIMPLE_STRING:
		str, err := ReadLine(reader)
		if err != nil {
			return RESP{}, err
		}
		return RESP{Type: RESP_SIMPLE_STRING, Str: str}, nil

	case RESP_ERROR:
		str, err := ReadLine(reader)
		if err != nil {
			return RESP{}, err
		}
		return RESP{Type: RESP_ERROR, Str: str}, nil

	case RESP_INTEGER:
		num, err := ReadInteger(reader)
		if err != nil {
			return RESP{}, err
		}
		return RESP{Type: RESP_INTEGER, Num: num}, nil

	case RESP_BULK_STRING:
		size, err := ReadInteger(reader)
		if err != nil {
			return RESP{}, err
		}

		if size == -1 {
			return RESP{Type: RESP_BULK_STRING, Num: -1}, nil
		}

		data := make([]byte, size+2) // +2 for CRLF
		_, err = io.ReadFull(reader, data)
		if err != nil {
			return RESP{}, err
		}

		return RESP{Type: RESP_BULK_STRING, Str: string(data[:size])}, nil

	case RESP_ARRAY:
		count, err := ReadInteger(reader)
		if err != nil {
			return RESP{}, err
		}

		if count == -1 {
			return RESP{Type: RESP_ARRAY, Num: -1}, nil
		}

		elements := make([]RESP, count)
		for i := 0; i < count; i++ {
			element, err := ParseRESP(reader)
			if err != nil {
				return RESP{}, err
			}
			elements[i] = element
		}

		return RESP{Type: RESP_ARRAY, Elements: elements}, nil

	default:
		return RESP{}, fmt.Errorf("unknown RESP type: %c", respType)
	}
}

// ReadLine reads a line ending with CRLF
func ReadLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}

// ReadInteger reads an integer followed by CRLF
func ReadInteger(reader *bufio.Reader) (int, error) {
	line, err := ReadLine(reader)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(line)
}

// CreateErrorResp creates an error RESP
func CreateErrorResp(message string) RESP {
	return RESP{Type: RESP_ERROR, Str: fmt.Sprintf("ERR %s", message)}
}

// SendRESP writes a RESP to a connection
func SendRESP(conn net.Conn, resp RESP) error {
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
			err = SendRESP(conn, element)
			if err != nil {
				return err
			}
		}

		return nil

	default:
		return fmt.Errorf("unknown RESP type: %c", resp.Type)
	}
}

// BuildRESPCommand creates a RESP array command string from given arguments
func BuildRESPCommand(args ...string) string {
	// Start with the array length
	result := fmt.Sprintf("*%d\r\n", len(args))

	// Add each argument as a bulk string
	for _, arg := range args {
		result += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}

	return result
}

// FormatREPLCONFPort formats a REPLCONF listening-port command
func FormatREPLCONFPort(port string) string {
	return BuildRESPCommand("REPLCONF", "listening-port", port)
}

// FormatREPLCONFCapa formats a REPLCONF capa command with multiple capabilities
func FormatREPLCONFCapa(capabilities ...string) string {
	// Build command with each capability as a separate argument:
	// REPLCONF capa <cap1> capa <cap2> ...
	args := []string{"REPLCONF"}

	for _, cap := range capabilities {
		args = append(args, "capa", cap)
	}

	return BuildRESPCommand(args...)
}

// FormatPSYNC formats a PSYNC command
func FormatPSYNC(replID string, offset string) string {
	return BuildRESPCommand("PSYNC", replID, offset)
}

// ValidateRESPCommand is a debug helper to print the raw bytes of a RESP command
func ValidateRESPCommand(cmd string) string {
	result := ""
	for i, b := range []byte(cmd) {
		if i > 0 {
			result += " "
		}
		result += fmt.Sprintf("%02X", b)
	}
	return result
}
