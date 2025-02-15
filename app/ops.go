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
