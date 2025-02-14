package main

import (
	"log"
	"net"
	"os"
)

type App struct {
	infoLogger  *log.Logger
	errorLogger *log.Logger
}

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// loggers
	infoLogger := log.New(os.Stdout, "INFO: ", log.Lshortfile)
	errorLogger := log.New(os.Stderr, "ERROR: ", log.Lshortfile)
	app := App{
		infoLogger:  infoLogger,
		errorLogger: errorLogger,
	}

	app.infoLogger.Println("server starting at port 6379")

	// establish socket connection
	listner, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		app.errorLogger.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		// start accepting connection on the socket address and port
		// INFO: blocking call
		connection, err := listner.Accept()
		if err != nil {
			app.errorLogger.Println("failed to accept connection: ", err.Error())
			os.Exit(1)
		}

		defer connection.Close()

		// handle the connection
		go app.handleConnection(connection)
	}
}

// ROLE: handle the connection
func (app *App) handleConnection(connection net.Conn) error {
	for {
		// read the connection input
		inputdata, err := app.readInput(connection)
		if err != nil {
			app.errorLogger.Println("failed to read input from client", err)
			return err
		}
		app.infoLogger.Println("recieved input from client")

		// parse the input using our own Redis RESP parser
		commands, err := app.respParser(inputdata)
		if err != nil {
			app.infoLogger.Println("failed to parse the input data", err)
			return err
		}

		// handle the commands accordingly
		dataToSend := app.handleCommands(commands)
		//dummeyDataToSend := []byte("+PONG\r\n")

		// write to the connection
		err = app.WriteToClient(connection, dataToSend)
		if err != nil {
			app.errorLogger.Println("failed to write the data to the client", err)
			return err
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
