package main

import (
	"bufio"
	"bytes"
	"strconv"
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
func (app *App) RESP(inputdata []byte) ([]string, error) {
	// 1. Read: Redis Data type -> Go Data Type
	commands, err := app.readRESP(inputdata)
	if err != nil {
		app.errorLogger.Println("failed to parse the input data", err)
		return nil, err
	}
	return commands, nil
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
