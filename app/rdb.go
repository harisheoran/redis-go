package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path"
	"time"
)

/*
ROLE: Serialize the data
*/
var crc64ECMATable = crc64.MakeTable(crc64.ECMA)

const (
	FA = 0xFA // AUX: Auxiliary Field, key-value settings
	FE = 0xFE // select DB: database selector
	FD = 0xFD // expire time in seconds
	FC = 0xFC // expire time in milliseconds
	FB = 0xFB // hash table sizes
	FF = 0xFF // end of the file

	// for main header section
	REDIS_VERSION = "0011"
	REDIS         = "REDIS"

	DB_INDEX = 7

	// Redis Value type
	STRING_TYPE     = 0x00
	LIST_TYPE       = 0x01
	SET_TYPE        = 0x02
	SORTED_SET_TYPE = 0x03
	HASH_TYPE       = 0x04
)

func (app *App) serializeRdbData() error {
	// check file
	rdbPath, err := app.checkRDBfile()
	if err != nil {
		return err
	}

	// write to the file
	err = app.writeRdbFile(rdbPath)
	if err != nil {
		return err
	}

	return nil
}

func (app *App) checkRDBfile() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	rdbPath := path.Join(homeDir, *dir, *dbFileName)

	// check file exist, if not then create the file
	_, err = os.Stat(rdbPath)
	if errors.Is(err, os.ErrNotExist) {
		// create the file
		_, err := os.Create(rdbPath)
		if err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	}

	return rdbPath, nil
}

func (app *App) writeRdbFile(rdbPath string) error {
	fmt.Println("starting writer...")
	var headers = [][]byte{
		[]byte(REDIS + REDIS_VERSION),
	}

	file, err := os.OpenFile(rdbPath, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	// 1. write headers
	for _, value := range headers {
		_, err = writer.Write(value)
		if err != nil {
			return err
		}
	}

	// 2. write metadata
	_, err = writer.Write([]byte{FA})
	if err != nil {
		return err
	}
	err = app.stringEncoding(writer, "redis-ver")
	if err != nil {
		return err
	}
	err = app.stringEncoding(writer, "6.0.0")
	if err != nil {
		return err
	}

	// 3. Database section
	_, err = writer.Write([]byte{FE})
	if err != nil {
		return err
	}
	// index of the database
	lenSizeByte, err := app.lengthEncoding(DB_INDEX)
	if err != nil {
		return err
	}
	_, err = writer.Write(lenSizeByte)
	if err != nil {
		return err
	}

	// 4. hashtable size information
	_, err = writer.Write([]byte{FB})
	if err != nil {
		return err
	}
	// actual size of the hashtable
	lenDbByte, err := app.lengthEncoding(len(db))
	if err != nil {
		return err
	}
	_, err = writer.Write(lenDbByte)
	if err != nil {
		return err
	}
	// expiry hashtable size
	lenExpiryTableSizeByte, err := app.lengthEncoding(app.getExpiryHashTableSize())
	if err != nil {
		return err
	}

	if _, err := writer.Write(lenExpiryTableSizeByte); err != nil {
		return err
	}

	// 6. actual key:pair valuesz
	for key, value := range db {
		if value.expiration.IsZero() {
			err := app.writeKeyValuePair(writer, key, value)
			if err != nil {
				return err
			}
		} else {
			// 1. Indicates that this key has an expire, ans it is in milliseconds
			_, err := writer.Write([]byte{FC})
			if err != nil {
				return err
			}
			// the expiry timestamp
			timestampByte, err := app.timestampEncoding(value.expiration)
			if err != nil {
				return err
			}
			if _, err = writer.Write(timestampByte); err != nil {
				return err
			}
			// write key value pair
			err = app.writeKeyValuePair(writer, key, value)
			if err != nil {
				return err
			}

		}
	}

	// 7. end of the rdb file
	if _, err := writer.Write([]byte{FF}); err != nil {
		return err
	}

	// 8. An 8-byte checksum of entire file
	checksumByte, err := app.calculateCRC64Checksum(rdbPath)
	if err != nil {
		return err
	}
	_, err = writer.Write(checksumByte)
	if err != nil {
		return err
	}

	err = writer.Flush()
	if err != nil {
		return err
	}
	return nil
}

func (app *App) writeKeyValuePair(writer io.Writer, key string, value Value) error {
	// 1. write value type
	_, err := writer.Write([]byte{byte(STRING_TYPE)})
	if err != nil {
		return err
	}
	// 2. write key (string encoded)
	err = app.stringEncoding(writer, key)
	if err != nil {
		return err
	}
	// 3. write value (string encoded)
	err = app.stringEncoding(writer, value.value)
	if err != nil {
		return err
	}
	return nil
}

// encode string as per Redis RDB and write the string
func (app *App) stringEncoding(w io.Writer, s string) error {
	lenBytes, err := app.lengthEncoding(len(s))
	if err != nil {
		return err
	}

	_, err = w.Write(lenBytes)
	if err != nil {
		return err
	}

	_, err = w.Write([]byte(s))
	if err != nil {
		return err
	}

	return nil
}

// encode lenght/size
func (app *App) lengthEncoding(length int) ([]byte, error) {
	if length < 0 {
		return nil, fmt.Errorf("invalid negative length")
	}

	// case 1: 1 byte length: 6 bit actual length and 00 represent it
	// 6 bit can take upto 64 length
	if length < 1<<6 {
		return []byte{byte(length)}, nil
	}

	// case 2: 2 byte length: 14 bit actual length and
	if length < 1<<14 {
		return []byte{
			byte(length>>8) | 0x40,
			byte(length),
		}, nil
	}

	// case 3: 5 byte length: last 4 byte(32 bit) actual length,
	// first byte is used to represent(only 2MSB bit, discard last 6 bits)
	if length <= 1<<32-1 {
		buffer := make([]byte, 5)
		buffer[0] = 0x80
		binary.LittleEndian.PutUint32(buffer[1:], uint32(length))
		return buffer, nil
	}
	return nil, fmt.Errorf("length too large %d", length)
}

// encode the timestamp in unix time in little endian
func (app *App) timestampEncoding(timestamp time.Time) ([]byte, error) {
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, uint64(timestamp.UnixMilli()))
	return buffer, nil
}

// create checkcum encoding
func (app *App) calculateCRC64Checksum(filepath string) ([]byte, error) {
	// Open the file
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	hash := crc64.New(crc64ECMATable)

	buffer := make([]byte, 64*1024)
	for {
		n, err := file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		hash.Write(buffer[:n])
	}

	// Get the checksum as a uint64
	checksum := hash.Sum64()

	// Encode the checksum as 8 bytes in little-endian format
	checksumBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(checksumBytes, checksum)

	return checksumBytes, nil
}

// ROLE: helper function to get size of hash table of expiry table
func (app *App) getExpiryHashTableSize() int {
	var size = 0
	for _, value := range db {
		if !value.expiration.IsZero() {
			size++
		}
	}
	return size
}

///////////////////////////////////////////////////
/*
ROLE: Deserialize the RDB data
*/
///////////////////////////////////////////////////
func (app *App) DeserializeData() error {
	// check file if exist
	rdbPath, err := app.checkRDBfile()
	if err != nil {
		return err
	}

	// open the file
	file, err := os.OpenFile(rdbPath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// create bufio reader to read the file
	reader := bufio.NewReader(file)

	// 1. check header to verify that is redis file
	headerBuffer := make([]byte, 9)
	if _, err = reader.Read(headerBuffer); err != nil {
		return err
	}
	if string(headerBuffer) != fmt.Sprintf("%s%s", REDIS, REDIS_VERSION) {
		return fmt.Errorf("rdb file is not a valid Redis file")
	}

	// 2. read the metadata section
	for {
		readedByte, err := reader.ReadByte()
		if err != nil {
			return err
		}

		if readedByte == FE {
			break
		}
	}

	// 3. read DB Index
	dbInd, err := reader.ReadByte()
	if err != nil {
		return err
	}
	app.infoLogger.Println("DBINDEX: ", dbInd)

	// 4. read the Hashtable Size
	if _, err = reader.ReadByte(); err != nil {
		return err
	}
	mainTableSize, err := reader.ReadByte()
	if err != nil {
		return err
	}
	app.infoLogger.Println("Main Table Size:", mainTableSize)

	// if table size is zero then no data
	//
	//
	//
	//
	//

	ttlHashTableSize, err := reader.ReadByte()
	if err != nil {
		return err
	}
	app.infoLogger.Println("TTL Hash Table Size", ttlHashTableSize)

	// 5. Actual Key:Value pairs
	for {
		readedByte, err := reader.ReadByte()
		if err != nil {
			return err
		}

		switch readedByte {
		case FC:
		case FD:
		default:
			app.DeserializeKeyValuePair(*reader)
		}
	}

	return nil
}

// helper
func (app *App) DeserializeKeyValuePair(reader bufio.Reader) error {
	// read type of the value
	valueTypeByte, err := reader.ReadByte()
	if err != nil {
		return err
	}

	switch valueTypeByte {
	case STRING_TYPE:
		app.helperDeserializeString(reader)
	case HASH_TYPE:
	case LIST_TYPE:
	case SORTED_SET_TYPE:
	case SET_TYPE:
	default:
	}

	return err
}

/*
Helper Function for Deserialization
*/
// 1. ROLE: Deseraialize the String types
func (app *App) helperDeserializeString(reader bufio.Reader) error {
	//mask = "0xC0"
	// decode the length of the KEY
	lengthBuffer := make([]byte, 1)
	if _, err := reader.Read(lengthBuffer); err != nil {
		return err
	}

	if lengthBuffer[0]&0xC0 == 0x00 {
		fmt.Println("HELL")
	}

	return nil
}

func (app *App) decodeLength() {

}
