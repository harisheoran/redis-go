package main

import (
	"encoding/hex"
	"fmt"
	"net"
	"strings"
)

// Send Handshake
func (app *App) SendHandshake() error {
	addressArr := strings.Split(*replicaof, " ")
	if len(addressArr) != 2 {
		return fmt.Errorf("--replicaof values are not valid.")
	}
	address := fmt.Sprintf("%s:%s", addressArr[0], addressArr[1])

	connection, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}

	// 1. send PING command to Master
	PING_COMMAND := "*1\r\n$4\r\nPING\r\n"
	_, err = connection.Write([]byte(PING_COMMAND))
	if err != nil {
		return err
	}
	app.infoLogger.Println("Successfully send the PING handshake")

	pingRes := make([]byte, 7)
	if _, err := connection.Read(pingRes); err != nil {
		return err
	}
	app.infoLogger.Println("Successfully received response from the PING handshake", string(pingRes))

	// 2. send REPLCONF command to master 2 times
	// First: it'll notify about port on which it(replica/slave) is listening on
	// Second: it'll send capabilities of the replica.
	replConfFirstArrayReq := app.createRESPArray([]string{"REPLCONF", "listening-port", *port})
	replConfSecondArrayReq := app.createRESPArray([]string{"REPLCONF", "capa", "psync2"})
	if _, err = connection.Write([]byte(replConfFirstArrayReq)); err != nil {
		return err
	}
	app.infoLogger.Println("Successfully send the first REPLCONF handshake")

	responseFirstREPLCONF := make([]byte, 5)
	if _, err = connection.Read(responseFirstREPLCONF); err != nil {
		return err
	}
	app.infoLogger.Println("Successfully recieved response from fisrt REPLCONF handshake", string(responseFirstREPLCONF))

	if _, err = connection.Write([]byte(replConfSecondArrayReq)); err != nil {
		return err
	}
	app.infoLogger.Println("Successfully send the second REPLCONF handshake")

	responseSecondREPLCONF := make([]byte, 5)
	if _, err = connection.Read(responseFirstREPLCONF); err != nil {
		return nil
	}
	app.infoLogger.Println("Successfully recieved response from the second REPLCONF handshake", string(responseSecondREPLCONF))

	psyncArrayReq := app.createRESPArray([]string{"PSYNC", "?", "-1"})
	if _, err := connection.Write([]byte(psyncArrayReq)); err != nil {
		return err
	}
	app.infoLogger.Println("Successfully send the PSYNC handshake")

	psyncRes := make([]byte, 54)
	if _, err = connection.Read(psyncRes); err != nil {
		return nil
	}
	app.infoLogger.Println("Successfully recieved response from the PSYNC handshake", string(psyncRes))

	rdbFileBuffer := make([]byte, 1024)
	if _, err = connection.Read(rdbFileBuffer); err != nil {
		return nil
	}
	app.infoLogger.Println("Successfully recieved the rdb file from master", string(rdbFileBuffer))

	go func(connection net.Conn) {
		defer connection.Close()
		for {
			buffer := make([]byte, 1024)
			if _, err = connection.Read(buffer); err != nil {
				app.errorLogger.Println("failed to read from master", err)
				return
			}
			app.infoLogger.Println("Successfully recieved SET from master", string(buffer))
		}
	}(connection)

	return nil
}

// send by master
func (app *App) createfullResyncRDBFileResponse() ([]byte, error) {
	//	$<length_of_file>\r\n<contents_of_file>
	hexContent := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

	byteContent, err := hex.DecodeString(hexContent)
	if err != nil {
		return nil, err
	}

	size := len(byteContent)

	response := fmt.Sprintf("$%d\r\n%s", size, string(byteContent))

	return []byte(response), nil
}
