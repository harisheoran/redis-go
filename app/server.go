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
	infoLogger := log.New(os.Stdout, "INFO", log.Lshortfile)
	errorLogger := log.New(os.Stderr, "ERROR", log.Lshortfile)
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
		// blocking call
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

func (app *App) handleConnection(connection net.Conn) {
	for {
		// read the connection input
		data, err := app.readInput(connection)
		if err != nil {
			app.errorLogger.Println("failed to read input from client", err)
		}

		app.infoLogger.Println("recieved input from client", string(data))

		// write
		connection.Write([]byte("+PONG\r\n"))
	}
}

func (app *App) readInput(connection net.Conn) ([]byte, error) {
	buffer := make([]byte, 1024)
	_, err := connection.Read(buffer)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}
