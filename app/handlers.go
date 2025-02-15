package main

import (
	"net"
	"os"
)

// ROLE: handle the connection
// Workflow: Read input -> RESP Parser -> Execute -> Write Output
func (app *App) handleConnection(connection net.Conn) {
	for {
		// 1. Read the input from the connection
		inputdata, err := app.readInput(connection)
		if err != nil {
			app.errorLogger.Println("failed to read input from client", err)
			os.Exit(1)
		}
		app.infoLogger.Println("recieved input from client")

		// 2. Parse the input using our own Redis RESP parser
		result, err := app.RESP(inputdata)
		if err != nil {
			app.errorLogger.Println("failed to parse data using RESP", err)
			os.Exit(1)
		}

		// 4. Write to the connection
		err = app.WriteToClient(connection, result)
		if err != nil {
			app.errorLogger.Println("failed to write the data to the connection", err)
			os.Exit(1)
		}
	}

}

// Role: read the data from the client/connection
func (app *App) readInput(connection net.Conn) ([]byte, error) {
	buffer := make([]byte, 1024)
	_, err := connection.Read(buffer)
	if err != nil {
		return nil, err
	}
	app.infoLogger.Println("Input: ", string(buffer))
	return buffer, nil
}

// Role: write data to the client/connection
func (app *App) WriteToClient(connection net.Conn, dataToSend []byte) error {
	_, err := connection.Write(dataToSend)
	if err != nil {
		return err
	}
	return nil
}
