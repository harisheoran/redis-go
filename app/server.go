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

	listner, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		app.errorLogger.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	_, err = listner.Accept()
	if err != nil {
		app.errorLogger.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
}
