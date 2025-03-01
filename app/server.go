package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
)

/*
INFO: Entry point of the server
*/

// map to store the data
var db = make(map[string]Value)

// dir and dbfilename flags
var dir = flag.String("dir", ".redis/rdb/", "Redis RDB file path")
var dbFileName = flag.String("dbfilename", "redis.rdb", "Redis RDB file name")
var port = flag.String("port", "6379", "Redis-Go server port")

func main() {
	// parse the flags
	flag.Parse()

	// loggers
	infoLogger := log.New(os.Stdout, "INFO: ", log.Lshortfile)
	errorLogger := log.New(os.Stderr, "ERROR: ", log.Lshortfile)
	app := App{
		infoLogger:  infoLogger,
		errorLogger: errorLogger,
	}

	err := app.DeserializeRDB()
	if err != nil {
		app.errorLogger.Println("THIS is the error ", err)
	}

	address := fmt.Sprintf("0.0.0.0:%s", *port)
	app.infoLogger.Println("server starting at port", address)
	// establish socket connection
	listner, err := net.Listen("tcp", address)
	if err != nil {
		app.errorLogger.Println("Failed to bind to port", address)
		os.Exit(1)
	}
	defer listner.Close()

	for {
		// start accepting connection on the socket address and port
		// INFO: blocking call
		connection, err := listner.Accept()
		if err != nil {
			app.errorLogger.Println("failed to accept connection: ", err.Error())
			os.Exit(1)
		}

		// handle the connection
		go app.handleConnection(connection)
	}
}
