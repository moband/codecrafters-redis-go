package main

import (
	"bufio"
	"fmt"
	"io"
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
