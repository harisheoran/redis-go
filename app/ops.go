package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

/*
// Write RESP Parser
*/
// Check the commands -> pass it to the executer(ops.go) -> get the result
func (app *App) ExecuteCommands(commands []string, connection net.Conn) error {
	mainCommand := commands[0]
	switch {
	case strings.EqualFold(mainCommand, "COMMAND"):
		return app.WriteToClient(connection, []byte("+PONG\r\n"))
	case strings.EqualFold(mainCommand, "PING"):
		return app.WriteToClient(connection, []byte("+PONG\r\n"))
	case strings.EqualFold(mainCommand, "ECHO"):
		return app.WriteToClient(connection, app.executeECHO(commands))
	case strings.EqualFold(mainCommand, "SET"):
		// if there is a slace replica -> send this command to replica
		// if there is no replica then execute it by myself
		if role == MASTER {
			app.infoLogger.Println("Sending commands to replicas")
			fmt.Println("Connection, ", slaveConnections)
			for _, conn := range slaveConnections {
				command := app.createRESPArray(commands)
				go func(conn net.Conn) {
					app.infoLogger.Println("Slave Commection Sending to ", &connection)
					if err := app.WriteToClient(conn, []byte(command)); err != nil {
						app.errorLogger.Println("failed to send the command to slave", err)
					}
				}(conn)
			}
			return nil
		}
		fmt.Println("HEY man")
		res, err := app.executeSET(commands)
		if err != nil {
			return err
		}
		return app.WriteToClient(connection, res)
	case strings.EqualFold(mainCommand, "GET"):
		return app.WriteToClient(connection, app.executeGET(commands))
	case strings.EqualFold(mainCommand, "CONFIG"):
		return app.WriteToClient(connection, app.executeCONFIG(commands))
	case strings.EqualFold(mainCommand, "KEYS"):
		return app.WriteToClient(connection, app.executeKEYS(commands))
	case strings.EqualFold(mainCommand, "save"):
		response, err := app.SAVE()
		if err != nil {
			app.errorLogger.Println(err)
		}
		return app.WriteToClient(connection, response)
	case strings.EqualFold(mainCommand, "info"):
		if len(commands) >= 2 && strings.EqualFold(commands[1], "replication") {
			return app.WriteToClient(connection, app.INFO())
		}
		return app.WriteToClient(connection, ErrorResponse)
	case strings.EqualFold(mainCommand, "REPLCONF"):
		return app.WriteToClient(connection, []byte("+OK\r\n"))
	case strings.EqualFold(mainCommand, "PSYNC"):
		slaveConnections = append(slaveConnections, connection)
		if err := app.WriteToClient(connection, app.executePSYNC()); err != nil {
			return err
		}
		// master operations
		if role == MASTER && isFULLRESYNC {
			fullResyncResponse, err := app.createfullResyncRDBFileResponse()
			if err != nil {
				app.errorLogger.Println("failed to send the FULLRESYNC rdb file to slave", err)
				return err
			}

			err = app.WriteToClient(connection, fullResyncResponse)
			if err != nil {
				app.errorLogger.Println("failed to write the data to the connection", err)
				return err
			}
			app.infoLogger.Println("Successfully send the RDB file for full resync.")

			isFULLRESYNC = false
		}
	default:
		return app.WriteToClient(connection, []byte("- ERR send a valid command\r\n"))
	}
	return nil
}

// ROLE: handle KEYS command
func (app *App) executeKEYS(commands []string) []byte {
	if len(commands) >= 2 && commands[1] == "*" {
		return app.KEY()
	}
	return []byte("-ERROR subcommand is missing\r\n")
}

// ROLE: handle echo command
func (app *App) executeECHO(commands []string) []byte {
	if len(commands) > 1 {
		size := len(commands[1])
		res := fmt.Sprintf("$%d\r\n%s\r\n", size, commands[1])
		return []byte(res)
	}
	return nil
}

// ROLE: Send Commands to command handler(ops.go) and send response
func (app *App) executeSET(commands []string) ([]byte, error) {
	if len(commands) >= 5 && strings.EqualFold(commands[3], "PX") {
		expiry, err := strconv.ParseInt(commands[4], 10, 64)
		if err != nil {
			return nil, err
		}
		return app.SET(
			commands[1],
			Value{
				value:      commands[2],
				expiration: time.Now().Add(time.Duration(expiry) * time.Millisecond),
			},
		), nil
	} else if len(commands) >= 3 {
		return app.SET(
			commands[1],
			Value{
				value: commands[2],
			},
		), nil
	}
	return []byte("-ERR not enough args: Key or Value missing\r\n"), nil
}

// ROLE: send commands to handler(ops.go) and get the response
func (app *App) executeGET(commands []string) []byte {
	if len(commands) >= 2 {
		return app.GET(commands[1])
	}
	return []byte("-ERR not enough args: Key missing\r\n")
}

// ROLE: handle CONFIG command
func (app *App) executeCONFIG(commands []string) []byte {
	if len(commands) == 1 {
		return []byte("- ERR send a valid command missing GET or SET\r\n")
	} else if len(commands) == 2 && strings.EqualFold(commands[1], "GET") {
		return []byte("- ERR send a valid command missing dir or dbfilename\r\n")
	} else if len(commands) == 2 && !strings.EqualFold(commands[1], "GET") {
		return []byte("- ERR send a valid command missing GET or SET\r\n")
	}

	if len(commands) == 3 && strings.EqualFold(commands[2], "dir") {
		return []byte(app.createRESPArray([]string{"dir", *dir}))
	} else if len(commands) == 3 && strings.EqualFold(commands[2], "dbfilename") {
		return []byte(app.createRESPArray([]string{"dbfilename", *dbFileName}))
	}

	return []byte("- ERR send a valid command\r\n")
}

func (app *App) executePSYNC() []byte {
	isFULLRESYNC = true
	response := fmt.Sprintf("+FULLRESYNC %s %s\r\n", MASTER_REPL_ID_VALUE, MASTER_REPL_OFFSET_VALUE)
	return []byte(response)
}

/*
RESP helper functions
*/
// 1. create a Redis protocol Array
func (app *App) createRESPArray(data []string) string {
	lengthOfArray := len(data)

	respArray := fmt.Sprintf("*%d\r\n", lengthOfArray)
	for i := 0; i < lengthOfArray; i++ {
		respArray = respArray + fmt.Sprintf("$%d\r\n%s\r\n", len(data[i]), string(data[i]))
	}

	return respArray
}

/*
INFO: Handle the execution of the commands
*/
// ROLE: handle the SET command
func (app *App) SET(key string, value Value) []byte {
	db[key] = value
	app.infoLogger.Println("DB:", db)
	successResponse := []byte("+OK\r\n")
	return successResponse
}

// ROLE: handle the GET command
func (app *App) GET(key string) []byte {
	value, ok := db[key]
	if !ok {
		return []byte("-1\r\n")
	}

	if time.Now().After(value.expiration) && !value.expiration.IsZero() {
		delete(db, key)
		return []byte("$-1\r\n")
	}
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value.value), value.value))
}

// ROLE: handle passive expiry
func (app *App) passiveExpiry(expiryTime time.Duration) bool {
	if time.Duration(time.Now().UnixMilli()) >= expiryTime {
		return true
	}
	return false
}

// ROLE: save the RDB file with the data
func (app *App) SAVE() ([]byte, error) {
	err := app.serializeRdbData()
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf("+File Saved in file %s at %s\r\n", *dbFileName, *dir)), nil
}

// get all elements using keys
func (app *App) KEY() []byte {
	var keysArray []string
	if len(db) > 0 {
		for keys, _ := range db {
			keysArray = append(keysArray, keys)
		}
		return []byte(app.createRESPArray(keysArray))
	}
	return []byte("-ERROR: no data is saved\r\n")
}

// INFO replication execution
func (app *App) INFO() []byte {
	rolePair := fmt.Sprintf("%s:%s\r\n", ROLE, role)
	masterIdPair := fmt.Sprintf("%s:%s\r\n", MASTER_REPL_ID, MASTER_REPL_ID_VALUE)
	masterOffsetPair := fmt.Sprintf("%s:%s", MASTER_REPL_OFFSET, MASTER_REPL_OFFSET_VALUE)

	return app.createBulkStringResponse(rolePair + masterIdPair + masterOffsetPair)
}

// ROLE: create bulk string response
func (app *App) createBulkStringResponse(responseStrings string) []byte {
	length := len(responseStrings)
	response := fmt.Sprintf("$%d\r\n%s\r\n", length, responseStrings)
	return []byte(response)
}
