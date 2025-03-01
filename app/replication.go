package main

import (
	"fmt"
	"net"
	"strings"
)

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
	defer connection.Close()

	PING_COMMAND := "*1\r\n$4\r\nPING\r\n"
	_, err = connection.Write([]byte(PING_COMMAND))
	if err != nil {
		return err
	}

	app.infoLogger.Println("Successfully send the PING handshake")

	return nil
}
