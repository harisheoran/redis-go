package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	SIMPLE_STRING = '+'
	ERROR         = '-'
	INTEGER       = ':'
	BULK_STRING   = '$'
	ARRAY         = '*'
)

var tempDb = make(map[string]string)

// Redis RESP Parser
// ROLE:
// The parser only converts raw input into structured data.
// It does not execute the command—this is handled separately.
func (app *App) respParser(input []byte) ([]string, error) {
	fmt.Println(string(input))

	// read the input bytes
	reader := bufio.NewReader(strings.NewReader(string(input)))

	// 1. read a single byte
	// start with first character which identify its Redis Type
	firstSymbol, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	// single quoted characters are of type rune which is an alias of int32
	// go will check this in ASCII value
	// byte is an alias of uint8 (0-255)
	// so we are comparing integer values
	switch firstSymbol {
	case ARRAY:
		return app.respHandleArray(reader)
	case BULK_STRING:
		app.respHandleBulkString()
	case INTEGER:

	case ERROR:

	case SIMPLE_STRING:

	}
	return nil, nil
}

// ROLE: Read the integer
// ex: $3, *13 $113
func (app *App) readInteger(reader *bufio.Reader) (int, error) {
	var fullNumber []byte
	for {
		number, err := reader.ReadByte()
		if err != nil {
			return -1, err
		}
		fullNumber = append(fullNumber, number)
		if string(fullNumber[len(fullNumber)-1]) == "\r" {
			fmt.Println("LENGTH is here:", len(string(fullNumber[:len(fullNumber)-1])), len(string(fullNumber[:len(fullNumber)-1])))
			break
		}
	}

	intLength, err := strconv.ParseInt(string(fullNumber[:len(fullNumber)-1]), 10, 64)
	if err != nil {
		return -1, err
	}

	return int(intLength), nil
}

func (app *App) respHandleArray(reader *bufio.Reader) ([]string, error) {
	commandArray := make([]string, 0)
	// 2. read the length of the Array
	// ex: *4 or *10 or *100
	length, err := app.readInteger(reader)
	if err != nil {
		return commandArray, err
	}

	fmt.Println("Length: ", length)

	// 3. read the end of lines after length of the Array
	// \n
	reader.ReadByte()

	fmt.Println("Intial length of array: ", len(commandArray))
	for i := 0; i < int(length); i++ {
		// 4. read the size symbol
		// ex: $
		sizeSymbol, err := reader.ReadByte()
		if err != nil {
			return commandArray, err
		}
		// check and exit here: wrong request
		if sizeSymbol != '$' {
			app.infoLogger.Println("invalid request, not as per redis protocol")
			os.Exit(1)
		} else {
			fmt.Println("Size symbol", string(sizeSymbol))
		}

		// 5. read the size of first element
		// ex: $4
		size, err := app.readInteger(reader)
		if err != nil {
			return commandArray, err
		}
		fmt.Println("Size: ", size)

		// 5. read the rest of the input: \r\n
		reader.ReadByte()

		// 6. read the actual first element
		element := make([]byte, size)
		_, err = reader.Read(element)
		if err != nil {
			return commandArray, err
		}

		fmt.Println("ADDING: ", string(element))
		// 7. add commands to our array/slice
		commandArray = append(commandArray, string(element))
		fmt.Println(commandArray, len(commandArray))

		// 8. read the last \r\n
		reader.ReadByte()
		reader.ReadByte()
	}

	return commandArray, nil
}

func (app *App) respHandleBulkString() {

}

/*
Write RESP Parser
*/

// get the output according to input
func (app *App) handleCommands(commands []string) []byte {
	mainCommand := commands[0]
	switch {
	case strings.EqualFold(mainCommand, "COMMAND"):
		return []byte("+PONG\r\n")
	case strings.EqualFold(mainCommand, "PING"):
		return []byte("+PONG\r\n")
	case strings.EqualFold(mainCommand, "ECHO"):
		if len(commands) > 1 {
			size := len(commands[1])
			res := fmt.Sprintf("$%d\r\n%s\r\n", size, commands[1])
			fmt.Println("THIS here", res)
			return []byte(res)
		}
		return nil
	case strings.EqualFold(mainCommand, "SET"):
		if len(commands) >= 3 {
			return app.SET(commands[1], commands[2])
		}
		return []byte("-ERR not enough args: Key or Value missing\r\n")
	case strings.EqualFold(mainCommand, "GET"):
		if len(commands) >= 2 {
			return app.GET(commands[1])
		}
		return []byte("-ERR not enough args: Key missing\r\n")
	default:
		return nil
	}
	return nil
}

// ROLE: handle the SET command
func (app *App) SET(key, value string) []byte {
	tempDb[key] = value
	successResponse := []byte("+OK\r\n")
	return successResponse
}

// ROLE: handle the GET command
func (app *App) GET(key string) []byte {
	value, ok := tempDb[key]
	if !ok {
		return []byte("-1\r\n")
	}

	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value))
}
