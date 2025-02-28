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
	rdbPath := path.Join(*dir, *dbFileName)

	// check file exist, if not then create the RDB file
	_, err := os.Stat(rdbPath)
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

	// 6. actual key:pair values
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
		app.infoLogger.Println("1 Byte length encoded")
		return []byte{byte(length)}, nil
	}

	// case 2: 2 byte length: 14 bit actual length
	if length < 1<<14 {
		app.infoLogger.Println("2 Byte length encoded")
		return []byte{
			byte(length>>8) | 0x40,
			byte(length),
		}, nil
	}

	// case 3: 5 byte length: last 4 byte(32 bit) actual length,
	// first byte is used to represent(only 2MSB bit, discard last 6 bits)
	if length <= 1<<32-1 {
		app.infoLogger.Println("5 Byte length decoded")
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
Read RDB file -> Decode Data -> Save it in-memory
*/
///////////////////////////////////////////////////
func (app *App) DeserializeRDB() error {
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
	reader := file

	// 1. check header to verify that is redis file
	headerBuffer := make([]byte, 9)
	if _, err = io.ReadFull(reader, headerBuffer); err != nil {
		return err
	}
	if string(headerBuffer) != fmt.Sprintf("%s%s", REDIS, REDIS_VERSION) {
		return fmt.Errorf("rdb file is not a valid Redis file")
	}

	// 2. read the metadata section
	for {
		readedByte := make([]byte, 1)
		_, err := io.ReadFull(reader, readedByte)
		if err != nil {
			return err
		}

		if readedByte[0] == FE {
			break
		}
	}

	// Database section
	// 3. read DB Index

	dbByte := make([]byte, 1)
	_, err = io.ReadFull(reader, dbByte)
	if err != nil {
		return err
	}
	app.infoLogger.Println("DBINDEX: ", dbByte)

	// 4. read the Hashtable Size
	// Reading the FB
	FBbyte := make([]byte, 1)
	if _, err = io.ReadFull(reader, FBbyte); err != nil {
		return err
	}
	// the actual size

	mainTableSize := make([]byte, 1)
	_, err = io.ReadFull(reader, mainTableSize)
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

	// read the size of hashtable
	ttlHashTableSizeByte := make([]byte, 1)
	_, err = io.ReadFull(reader, ttlHashTableSizeByte)
	if err != nil {
		return err
	}
	app.infoLogger.Println("TTL Hash Table Size", ttlHashTableSizeByte)

	// 5. Actual Key:Value pairs
	for i := 0; i < int(mainTableSize[0]); i++ {
		// case 1: can be FC or FD (tells about timeout expiry)
		// case 2: if it is not with time expiry than it is value type byte
		readedByte := make([]byte, 1)
		_, err := io.ReadFull(reader, readedByte)
		if err != nil {
			return err
		}

		// check FC or FD for if it has expiry time with Key Value pair
		switch readedByte[0] {
		case FC:
			app.infoLogger.Println("FC Type")
			key, value, err := app.helperDeserializeExpiryKeyValue(reader)
			if err != nil {
				app.errorLogger.Println("Failed to deserialize", err)
				return err
			}
			app.infoLogger.Println("Saving FC key value pair from RDB file...", "KEY:", key, "VALUE:", value)
			db[key] = value
		case FD:
			app.infoLogger.Println("FD type")
			key, value, err := app.helperDeserializeExpiryKeyValue(reader)
			if err != nil {
				app.errorLogger.Println("Failed to deserialize", err)
				return err
			}
			app.infoLogger.Println("Saving FD key value pair from RDB file...", "KEY:", key, "VALUE:", value)
			db[key] = value
		default:
			app.infoLogger.Println("Without expiry: ", readedByte)
			key, value, err := app.helperDeserailizeKeyValue(reader, readedByte)
			if err != nil {
				app.errorLogger.Println("Failed to deserialize", err)
				return err
			}
			app.infoLogger.Println("saving key value pair from RDB file...", "KEY:", key, "VALUE:", value.value)
			db[key] = value
		}
	}
	return nil
}

/*
Helper Functions for Deserialization
*/
// ROLE: Helper
// 1. Deserialize Key Value Pair which have expiry timeout
func (app *App) helperDeserializeExpiryKeyValue(reader io.Reader) (string, Value, error) {
	// read the timestamp
	timeStampByteBuffer := make([]byte, 8)
	_, err := io.ReadFull(reader, timeStampByteBuffer)
	if err != nil {
		return "", Value{}, err
	}
	timeExpiryBinary := binary.LittleEndian.Uint64(timeStampByteBuffer)
	timeExpiry := time.UnixMilli(int64(timeExpiryBinary))
	app.infoLogger.Println("Decoded time", timeExpiry)

	// read the value type byte
	valueTypeByte := make([]byte, 1)
	_, err = io.ReadFull(reader, valueTypeByte)
	if err != nil {
		return "", Value{}, err
	}
	app.infoLogger.Println("Value type Byte", valueTypeByte)

	// decode the key and value
	key, value, err := app.helperDeserailizeKeyValue(reader, valueTypeByte)
	if err != nil {
		return "", Value{}, err
	}

	value.expiration = timeExpiry

	return key, value, nil
}

// ROLE: Helper
// 2. Deserialize KEY VALUE pair
func (app *App) helperDeserailizeKeyValue(reader io.Reader, valueTypeByte []byte) (string, Value, error) {
	// read the key
	key, err := app.helperDeserializeString(reader)
	if err != nil {
		return "", Value{}, err
	}
	app.infoLogger.Println("KEY decoded:", key)

	var value string
	// read the value
	switch valueTypeByte[0] {
	case STRING_TYPE:
		app.infoLogger.Println("Decoding... Found String type")
		value, err = app.helperDeserializeString(reader)
		if err != nil {
			return "", Value{}, err
		}
		app.infoLogger.Println("VALUE decoded:", value)
	case HASH_TYPE:
	case LIST_TYPE:
	case SORTED_SET_TYPE:
	case SET_TYPE:
	default:
		app.infoLogger.Println("Not a valid type to decode")
	}

	valueData := Value{
		value: value,
	}
	return key, valueData, nil
}

// ROLE: Helper
// 3. Deseraialize the String types helper
func (app *App) helperDeserializeString(reader io.Reader) (string, error) {
	length, err := app.helperdecodeLength(reader)
	if err != nil {
		return "", err
	}
	app.infoLogger.Println("Decoded Length:", length)

	stringByte := make([]byte, length)
	if _, err = io.ReadFull(reader, stringByte); err != nil {
		return "", err
	}

	return string(stringByte), nil
}

// ROLE: Helper
// to decode the length
func (app *App) helperdecodeLength(reader io.Reader) (int, error) {
	// read the first byte of the length
	buffer := make([]byte, 1)
	if _, err := io.ReadFull(reader, buffer); err != nil {
		return -1, err
	}

	firstByte := buffer[0]
	// AND with 1100 0000 we will MSB bits
	prefixByte := firstByte & 0xC0
	switch prefixByte {
	case 0x00:
		app.infoLogger.Println("1 Byte length decoded")
		return int(firstByte & 0x3F), nil
	case 0x40:
		app.infoLogger.Println("2 Byte length decoded")
		nextByte := make([]byte, 1)
		_, err := io.ReadFull(reader, nextByte)
		if err != nil {
			return -1, err
		}
		// prefixByte & 0x3F : it gives 6 bit and remove MSB of 01 which
		// is not needed in actual length
		// << 8 : moves the 6 bit left side and make it 16 bit and
		// the OR with next byte gives full length byte
		return int(firstByte&0x3F)<<8 | int(nextByte[0]), nil
	case 0x80:
		app.infoLogger.Println("5 Byte length decoded")
		nextBytes := make([]byte, 4)
		if _, err := io.ReadFull(reader, nextBytes); err != nil {
			return -1, err
		}
		return int(binary.LittleEndian.Uint32(nextBytes)), nil
	default:
		return -1, fmt.Errorf("Invalid Length")
	}
}
