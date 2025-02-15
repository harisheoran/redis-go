package main

import (
	"log"
	"time"
)

/*
Contains all the custom classes
*/

// for main Application
type App struct {
	infoLogger  *log.Logger
	errorLogger *log.Logger
}

// for value used in saving KEY:VALUE pair
type Value struct {
	value      string
	expiration time.Time
}
