package main

import (
	"io"
	"net"
)

// ROLE: handle the connection
// Workflow: Read input -> RESP Parser -> Execute -> Write Output
func (app *App) handleConnection(connection net.Conn) {
	defer connection.Close()
	for {
		// 1. Read the input from the connection
		inputdata, err := app.readInput(connection)
		if err != nil {
			if err == io.EOF {
				app.errorLogger.Println("client closed the connection", err)
			} else {
				app.errorLogger.Println("failed to read input from client", err)
			}
			return
		}
		app.infoLogger.Println("recieved input from client", string(inputdata))

		// 2. Parse the input using our own Redis RESP parser
		result, err := app.RESP(inputdata)
		if err != nil {
			app.errorLogger.Println("failed to parse data using RESP", err)
			return
		}

		app.infoLogger.Println("RESP: Write result", string(result))

		// 4. Write to the connection
		err = app.WriteToClient(connection, result)
		if err != nil {
			app.errorLogger.Println("failed to write the data to the connection", err)
			return
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
