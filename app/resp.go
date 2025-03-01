package main

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	SIMPLE_STRING = '+'
	ERROR         = '-'
	INTEGER       = ':'
	BULK_STRING   = '$'
	ARRAY         = '*'
)

var ErrorResponse = []byte("- ERR send a valid command\r\n")

// Redis RESP Parser
// ROLE: The parser only converts raw input into structured data.
// It does not execute the command, commands are handled separately(ops.go).
func (app *App) RESP(inputdata []byte) ([]byte, error) {
	// 1. Read: Redis Data type -> Go Data Type
	commands, err := app.readRESP(inputdata)
	if err != nil {
		app.errorLogger.Println("failed to parse the input data", err)
		return nil, err
	}

	// 2. Write: Go Data Type -> Execution Handler -> Get response/result
	result, err := app.writeRESP(commands)
	if err != nil {
		app.errorLogger.Println("failed to handle the input commands", err)
		return nil, err
	}

	return result, nil
}

// ROLE: Read parser
func (app *App) readRESP(input []byte) ([]string, error) {
	// read the input bytes
	reader := bufio.NewReader(bytes.NewReader(input))

	// 1. read a single byte
	// start with first character which identify its Redis Data Type
	firstSymbol, err := reader.ReadByte()
	if err != nil {
		app.errorLogger.Println("failed to read the firstSymbol", err)
		return nil, err
	}
	// single quoted characters are of type rune which is an alias of int32
	// go will check this in ASCII value
	// byte is an alias of uint8 (0-255)
	// so we are comparing integer values
	switch firstSymbol {
	case ARRAY:
		app.infoLogger.Println("RESP: Array type input")
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
			break
		}
	}

	intLength, err := strconv.ParseInt(string(fullNumber[:len(fullNumber)-1]), 10, 64)
	if err != nil {
		return -1, err
	}

	return int(intLength), nil
}

// ROLE: read Redis Array Data type
func (app *App) respHandleArray(reader *bufio.Reader) ([]string, error) {
	commandArray := make([]string, 0)
	// 2. read the length of the Array
	// ex: *4 or *10 or *100
	length, err := app.readInteger(reader)
	if err != nil {
		app.errorLogger.Println("failed to read the length of array", err)
		return commandArray, err
	}

	// 3. read the end of lines after length of the Array
	// \n
	reader.ReadByte()

	for i := 0; i < int(length); i++ {
		// 4. read the size symbol
		// ex: $
		sizeSymbol, err := reader.ReadByte()
		if err != nil {
			app.errorLogger.Println("failed to read the size symbol", err)
			return commandArray, err
		}
		// check and exit here: wrong request
		if sizeSymbol != '$' {
			app.infoLogger.Println("invalid request, not as per redis protocol")
			return nil, err
		}

		// 5. read the size of element
		// ex: $4
		size, err := app.readInteger(reader)
		if err != nil {
			app.errorLogger.Println("failed to read the size of the element", err)
			return commandArray, err
		}

		// 5. read the rest of the input: \r\n
		reader.ReadByte()

		// 6. read the actual first element
		element := make([]byte, size)
		_, err = reader.Read(element)
		if err != nil {
			app.errorLogger.Println("failed to read the element", err)
			return commandArray, err
		}

		// 7. add commands to our array/slice
		commandArray = append(commandArray, string(element))

		// 8. read the last \r\n
		reader.ReadByte()
		reader.ReadByte()
	}

	return commandArray, nil
}

func (app *App) respHandleBulkString() {

}

/*
// Write RESP Parser
*/

// Check the commands -> pass it to the executer(ops.go) -> get the result
func (app *App) writeRESP(commands []string) ([]byte, error) {
	mainCommand := commands[0]
	switch {
	case strings.EqualFold(mainCommand, "COMMAND"):
		return []byte("+PONG\r\n"), nil
	case strings.EqualFold(mainCommand, "PING"):
		return []byte("+PONG\r\n"), nil
	case strings.EqualFold(mainCommand, "ECHO"):
		return app.writeRESP_ECHO(commands)
	case strings.EqualFold(mainCommand, "SET"):
		return app.writeRESP_SET(commands)
	case strings.EqualFold(mainCommand, "GET"):
		return app.writeRESP_GET(commands)
	case strings.EqualFold(mainCommand, "CONFIG"):
		return app.writeRESP_CONFIG(commands), nil
	case strings.EqualFold(mainCommand, "KEYS"):
		return app.writeRESP_KEYS(commands)
	case strings.EqualFold(mainCommand, "save"):
		response, err := app.SAVE()
		return response, err
	case strings.EqualFold(mainCommand, "info"):
		if len(commands) >= 2 && strings.EqualFold(commands[1], "replication") {
			return app.INFO(), nil
		}
		return ErrorResponse, nil
	case strings.EqualFold(mainCommand, "REPLCONF"):
		fmt.Println("Reaching here")
		return []byte("+OK\r\n"), nil
	default:
		return []byte("- ERR send a valid command\r\n"), nil
	}
}

// ROLE: handle KEYS command
func (app *App) writeRESP_KEYS(commands []string) ([]byte, error) {
	if len(commands) >= 2 && commands[1] == "*" {
		return app.KEY(), nil
	} else {
		return []byte("-ERROR subcommand is missing\r\n"), nil
	}
}

// ROLE: handle echo command
func (app *App) writeRESP_ECHO(commands []string) ([]byte, error) {
	if len(commands) > 1 {
		size := len(commands[1])
		res := fmt.Sprintf("$%d\r\n%s\r\n", size, commands[1])
		return []byte(res), nil
	}
	return nil, nil
}

// ROLE: Send Commands to command handler(ops.go) and send response
func (app *App) writeRESP_SET(commands []string) ([]byte, error) {
	if len(commands) >= 5 && strings.EqualFold(commands[3], "PX") {
		expiry, err := strconv.ParseInt(commands[4], 10, 64)
		if err != nil {
			return nil, err
		}
		app.infoLogger.Println("Input Time:", time.Now().Add(time.Duration(expiry)*time.Millisecond))
		return app.SET(
			commands[1],
			Value{
				value:      commands[2],
				expiration: time.Now().Add(time.Duration(expiry) * time.Millisecond),
			},
		), nil
	} else if len(commands) >= 3 {
		return app.SET(
			commands[1],
			Value{
				value: commands[2],
			},
		), nil
	}
	return []byte("-ERR not enough args: Key or Value missing\r\n"), nil
}

// ROLE: send commands to handler(ops.go) and get the response
func (app *App) writeRESP_GET(commands []string) ([]byte, error) {
	if len(commands) >= 2 {
		return app.GET(commands[1]), nil
	}
	return []byte("-ERR not enough args: Key missing\r\n"), nil
}

// ROLE: handle CONFIG command
func (app *App) writeRESP_CONFIG(commands []string) []byte {
	if len(commands) == 1 {
		return []byte("- ERR send a valid command missing GET or SET\r\n")
	} else if len(commands) == 2 && strings.EqualFold(commands[1], "GET") {
		return []byte("- ERR send a valid command missing dir or dbfilename\r\n")
	} else if len(commands) == 2 && !strings.EqualFold(commands[1], "GET") {
		return []byte("- ERR send a valid command missing GET or SET\r\n")
	}

	if len(commands) == 3 && strings.EqualFold(commands[2], "dir") {
		return []byte(app.createRESPArray([]string{"dir", *dir}))
	} else if len(commands) == 3 && strings.EqualFold(commands[2], "dbfilename") {
		return []byte(app.createRESPArray([]string{"dbfilename", *dbFileName}))
	}

	return []byte("- ERR send a valid command\r\n")
}

/*
RESP helper functions
*/
// 1. create a Redis protocol Array
func (app *App) createRESPArray(data []string) string {
	lengthOfArray := len(data)

	respArray := fmt.Sprintf("*%d\r\n", lengthOfArray)
	for i := 0; i < lengthOfArray; i++ {
		respArray = respArray + fmt.Sprintf("$%d\r\n%s\r\n", len(data[i]), string(data[i]))
	}

	return respArray
}
