package main

import (
	"log"
	"net"
	"os"
)

/*
INFO: Entry point of the server
*/

// map to store the data
var db = make(map[string]Value)

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
