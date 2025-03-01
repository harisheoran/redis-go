package main

import (
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
	//defer connection.Close()

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

	return nil
}
