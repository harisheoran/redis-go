package main

import (
	"fmt"
	"time"
)

/*
INFO: Handle the execution of the commands
*/

// ROLE: handle the SET command
func (app *App) SET(key string, value Value) []byte {
	db[key] = value
	app.infoLogger.Println("DB:", db)

	for _, value := range db {
		fmt.Println("HERE", value.expiration)
	}
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
		return []byte(app.createArrayResponse(keysArray))
	}
	return []byte("-ERROR: no data is saved\r\n")
}
