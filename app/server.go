package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

/*
INFO: Entry point of the server
*/

// map to store the data
var (
	db           = make(map[string]Value)
	isFULLRESYNC = false
	// be default
	role             = MASTER
	masterConnection net.Conn
	slaveConnections = []net.Conn{}
	// flags
	dir        = flag.String("dir", ".redis/rdb/", "Redis RDB file path")
	dbFileName = flag.String("dbfilename", "redis.rdb", "Redis RDB file name")
	port       = flag.String("port", "6379", "Redis-Go server port")
	replicaof  = flag.String("replicaof", "localhost 6379", "info about the master redis-go replica")
)

const (
	MASTER                   = "master"
	SLAVE                    = "slave"
	ROLE                     = "role"
	MASTER_REPL_ID           = "master_replid"
	MASTER_REPL_OFFSET       = "master_repl_offset"
	MASTER_REPL_ID_VALUE     = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	MASTER_REPL_OFFSET_VALUE = "0"
)

func main() {
	// parse the flags
	flag.Parse()

	flag.Visit(func(f *flag.Flag) {
		if f.Name == "replicaof" {
			role = SLAVE
		}
	})

	prefixInfo := fmt.Sprintf("INFO: %s ", strings.ToUpper(role))
	prefixError := fmt.Sprintf("ERROR: %s ", strings.ToUpper(role))
	// loggers
	infoLogger := log.New(os.Stdout, prefixInfo, log.Lshortfile)
	errorLogger := log.New(os.Stderr, prefixError, log.Lshortfile)
	app := App{
		infoLogger:  infoLogger,
		errorLogger: errorLogger,
	}

	if role == SLAVE {
		err := app.SendHandshake()
		if err != nil {
			app.errorLogger.Println("failed to send the handshake", err)
			return
		}
	}

	err := app.DeserializeRDB()
	if err != nil {
		app.errorLogger.Println("failed to deserialize the rdb file", err)
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
