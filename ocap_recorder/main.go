package main

/*
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "extensionCallback.h"
*/
import "C" // This is required to import the C code

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"ocap_recorder/defs"

	"github.com/glebarez/sqlite"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var EXTENSION_VERSION string = "0.0.1"
var extensionCallbackFnc C.extensionCallback

var ADDON string = "OCAP"
var EXTENSION string = "ocap_recorder"

// file paths
var ADDON_FOLDER string = getDir() + "\\@" + ADDON
var LOG_FILE string = ADDON_FOLDER + "\\" + EXTENSION + ".log"
var CONFIG_FILE string = ADDON_FOLDER + "\\config.json"
var LOCAL_DB_FILE string = ADDON_FOLDER + "\\ocap_recorder.db"

// global variables
var SAVE_LOCAL bool = false
var DB *gorm.DB
var DB_VALID bool = false
var sqlDB *sql.DB

var (
	// channels for receiving new data and filing to DB
	newSoldierChan      chan []string = make(chan []string, 15000)
	newVehicleChan      chan []string = make(chan []string, 15000)
	newSoldierStateChan chan []string = make(chan []string, 30000)
	newVehicleStateChan chan []string = make(chan []string, 30000)
	newFiredEventChan   chan []string = make(chan []string, 30000)

	// caches of processed models pending DB write
	soldiersToWrite      defs.SoldiersQueue      = defs.SoldiersQueue{}
	soldierStatesToWrite defs.SoldierStatesQueue = defs.SoldierStatesQueue{}
	vehiclesToWrite      defs.VehiclesQueue      = defs.VehiclesQueue{}
	vehicleStatesToWrite defs.VehicleStatesQueue = defs.VehicleStatesQueue{}
	firedEventsToWrite   defs.FiredEventsQueue   = defs.FiredEventsQueue{}

	// testing
	TEST_DATA         bool = false
	TEST_DATA_TIMEINC      = defs.SafeCounter{}

	// sqlite flow
	PAUSE_INSERTS bool = false

	SESSION_START_TIME  time.Time = time.Now()
	LAST_WRITE_DURATION time.Duration
)

type APIConfig struct {
	ServerURL string `json:"serverUrl"`
	APIKey    string `json:"apiKey"`
}

type DBConfig struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
}

type ConfigJson struct {
	Debug      bool      `json:"debug"`
	DefaultTag string    `json:"defaultTag"`
	LogsDir    string    `json:"logsDir"`
	APIConfig  APIConfig `json:"api"`
	DBConfig   DBConfig  `json:"db"`
}

var activeSettings ConfigJson = ConfigJson{}

// configure log output
func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// check if parent folder exists
	// if it doesn't, create it
	if _, err := os.Stat(ADDON_FOLDER); os.IsNotExist(err) {
		os.Mkdir(ADDON_FOLDER, 0755)
	}
	// check if LOG_FILE exists
	// if it does, move it to LOG_FILE.old
	// if it doesn't, create it
	if _, err := os.Stat(LOG_FILE); err == nil {
		os.Rename(LOG_FILE, LOG_FILE+".old")
	}
	f, err := os.OpenFile(LOG_FILE, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}

	log.SetOutput(f)

	loadConfig()

	LOG_FILE = fmt.Sprintf(`%s\%s.log`, activeSettings.LogsDir, EXTENSION)
	log.Println("Log location specified in config as", LOG_FILE)
	LOCAL_DB_FILE = fmt.Sprintf(`%s\%s_%s.db`, ADDON_FOLDER, EXTENSION, SESSION_START_TIME.Format("20060102_150405"))

	// resolve path set in activeSettings.LogsDir
	// create logs dir if it doesn't exist
	if _, err := os.Stat(activeSettings.LogsDir); os.IsNotExist(err) {
		os.Mkdir(activeSettings.LogsDir, 0755)
	}
	// log to file
	f, err = os.OpenFile(LOG_FILE, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	log.SetOutput(f)
}

func version() {
	functionName := "version"
	writeLog(functionName, fmt.Sprintf(`ocap_recorder version: %s`, EXTENSION_VERSION), "INFO")
}

func getDir() string {
	dir, err := os.Getwd()
	if err != nil {
		writeLog("getDir", fmt.Sprintf(`Error getting working directory: %v`, err), "ERROR")
		return ""
	}
	return dir
}

func loadConfig() {
	// load config from file as JSON
	functionName := "loadConfig"

	file, err := os.OpenFile(CONFIG_FILE, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`%s`, err), "ERROR")
		return
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&activeSettings)
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`%s`, err), "ERROR")
		return
	}

	checkServerStatus()

	writeLog(functionName, `Config loaded`, "INFO")
}

func checkServerStatus() {
	functionName := "checkServerStatus"
	var err error

	// check if server is running by making a healthcheck API request
	// if server is not running, log error and exit
	_, err = http.Get(activeSettings.APIConfig.ServerURL + "/healthcheck")
	if err != nil {
		writeLog(functionName, "OCAP Frontend is offline", "WARN")
	} else {
		writeLog(functionName, "OCAP Frontend is online", "INFO")
	}
}

///////////////////////
// DATABASE OPS //
///////////////////////

func getLocalDB() (err error) {
	functionName := "getDB"
	// connect to database (SQLite)
	DB, err = gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		PrepareStmt:            true,
		SkipDefaultTransaction: true,
		CreateBatchSize:        2000,
		Logger:                 logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		DB_VALID = false
		return err
	} else {
		writeLog(functionName, "Using local SQlite DB", "INFO")

		// set PRAGMAS
		err = DB.Exec("PRAGMA user_version = 1;").Error
		if err != nil {
			writeLog(functionName, "Error setting user_version PRAGMA", "ERROR")
			return err
		}
		err = DB.Exec("PRAGMA journal_mode = MEMORY;").Error
		if err != nil {
			writeLog(functionName, "Error setting journal_mode PRAGMA", "ERROR")
			return err
		}
		err = DB.Exec("PRAGMA synchronous = OFF;").Error
		if err != nil {
			writeLog(functionName, "Error setting synchronous PRAGMA", "ERROR")
			return err
		}
		err = DB.Exec("PRAGMA cache_size = -32000;").Error
		if err != nil {
			writeLog(functionName, "Error setting cache_size PRAGMA", "ERROR")
			return err
		}
		err = DB.Exec("PRAGMA temp_store = MEMORY;").Error
		if err != nil {
			writeLog(functionName, "Error setting temp_store PRAGMA", "ERROR")
			return err
		}

		err = DB.Exec("PRAGMA page_size = 32768;").Error
		if err != nil {
			writeLog(functionName, "Error setting page_size PRAGMA", "ERROR")
			return err
		}

		err = DB.Exec("PRAGMA mmap_size = 30000000000;").Error
		if err != nil {
			writeLog(functionName, "Error setting mmap_size PRAGMA", "ERROR")
			return err
		}

		DB_VALID = true
		return nil
	}
}

func dumpMemoryDBToDisk() (err error) {
	functionName := "dumpMemoryDBToDisk"
	// remove existing file if it exists
	exists, err := os.Stat(LOCAL_DB_FILE)
	if err == nil {
		if exists != nil {
			err = os.Remove(LOCAL_DB_FILE)
			if err != nil {
				writeLog(functionName, "Error removing existing DB file", "ERROR")
				return err
			}
		}
	}

	// dump memory DB to disk
	start := time.Now()
	err = DB.Exec("VACUUM INTO 'file:" + LOCAL_DB_FILE + "';").Error
	if err != nil {
		writeLog(functionName, "Error dumping memory DB to disk", "ERROR")
		return err
	}
	writeLog(functionName, fmt.Sprintf(`Dumped memory DB to disk in %s`, time.Since(start)), "INFO")
	return nil
}

func connectMySql() (err error) {
	// connect to database (MySQL/MariaDB)
	dsn := fmt.Sprintf(`%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True`,
		activeSettings.DBConfig.Username,
		activeSettings.DBConfig.Password,
		activeSettings.DBConfig.Host,
		activeSettings.DBConfig.Port,
		activeSettings.DBConfig.Database,
	)

	// wrap with gorm
	DB, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
		PrepareStmt:            true,
		SkipDefaultTransaction: true,
		CreateBatchSize:        2000,
		Logger:                 logger.Default.LogMode(logger.Silent),
	})

	if err != nil {
		writeLog("connectMySql", fmt.Sprintf(`Failed to connect to MySQL/MariaDB. Err: %s`, err), "ERROR")
		DB_VALID = false
		return
	} else {
		writeLog("connectMySql", "Connected to MySQL/MariaDB", "INFO")
		DB_VALID = true
		return
	}

}

// getDB connects to the MySql/MariaDB database, and if it fails, it will use a local SQlite DB
func getDB() (err error) {
	functionName := "getDB"

	// connect to database (Postgres) using gorm
	dsn := fmt.Sprintf(`host=%s port=%s user=%s password=%s dbname=%s sslmode=disable`,
		activeSettings.DBConfig.Host,
		activeSettings.DBConfig.Port,
		activeSettings.DBConfig.Username,
		activeSettings.DBConfig.Password,
		activeSettings.DBConfig.Database,
	)

	DB, err = gorm.Open(postgres.New(postgres.Config{
		DSN:                  dsn,
		PreferSimpleProtocol: true, // disables implicit prepared statement usage
	}), &gorm.Config{
		// PrepareStmt:            true,
		SkipDefaultTransaction: true,
		CreateBatchSize:        10000,
		Logger:                 logger.Default.LogMode(logger.Silent),
	})
	configValid := err == nil
	if !configValid {
		writeLog(functionName, fmt.Sprintf(`Failed to set up TimescaleDB. Err: %s`, err), "ERROR")
		SAVE_LOCAL = true
		getLocalDB()
	}
	// test connection
	err = DB.Exec(`SELECT 1`).Error
	connectionValid := err == nil
	if !connectionValid {
		writeLog(functionName, fmt.Sprintf(`Failed to connect to TimescaleDB. Err: %s`, err), "ERROR")
		SAVE_LOCAL = true
		err = getLocalDB()
		if err != nil {
			writeLog(functionName, fmt.Sprintf(`Failed to connect to SQLite. Err: %s`, err), "ERROR")
			DB_VALID = false
		} else {
			DB_VALID = true
		}
	} else {
		writeLog(functionName, "Connected to TimescaleDB", "INFO")
		SAVE_LOCAL = false
		DB_VALID = true
	}

	if !DB_VALID {
		writeLog(functionName, "DB not valid. Not saving!", "ERROR")
		return errors.New("DB not valid. Not saving")
	}

	if !SAVE_LOCAL {
		// Ensure PostGIS and TimescaleDB extensions are installed
		err = DB.Exec(`
		CREATE EXTENSION IF NOT EXISTS postgis;
		`).Error
		if err != nil {
			writeLog(functionName, fmt.Sprintf(`Failed to create PostGIS extension. Err: %s`, err), "ERROR")
			DB_VALID = false
			return err
		} else {
			writeLog(functionName, "PostGIS extension created", "INFO")
		}
		err = DB.Exec(`
		CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
		`).Error
		if err != nil {
			writeLog(functionName, fmt.Sprintf(`Failed to create TimescaleDB extension. Err: %s`, err), "ERROR")
			DB_VALID = false
			return err
		} else {
			writeLog(functionName, "TimescaleDB extension created", "INFO")
		}
	}

	// Check if OcapInfo table exists
	if !DB.Migrator().HasTable(&defs.OcapInfo{}) {
		// Create the table
		err = DB.AutoMigrate(&defs.OcapInfo{})
		if err != nil {
			writeLog(functionName, fmt.Sprintf(`Failed to create ocap_info table. Err: %s`, err), "ERROR")
			DB_VALID = false
			return err
		}
		// Create the default settings
		err = DB.Create(&defs.OcapInfo{
			GroupName:        "OCAP",
			GroupDescription: "OCAP",
			GroupLogo:        "https://i.imgur.com/0Q4z0ZP.png",
			GroupWebsite:     "https://ocap.arma3.com",
		}).Error

		if err != nil {
			writeLog(functionName, fmt.Sprintf(`Failed to create ocap_info entry. Err: %s`, err), "ERROR")
			DB_VALID = false
			return err
		}
	}

	/////////////////////////////
	// Migrate the schema
	/////////////////////////////

	toMigrate := make([]interface{}, 0)
	toMigrate = append(toMigrate, &defs.World{})
	toMigrate = append(toMigrate, &defs.Mission{})
	toMigrate = append(toMigrate, &defs.Soldier{})
	toMigrate = append(toMigrate, &defs.Vehicle{})
	conditionalMigrate := map[string]interface{}{
		"soldier_states": &defs.SoldierState{},
		"vehicle_states": &defs.VehicleState{},
		"fired_events":   &defs.FiredEvent{},
	}
	var existingHypertablesNames []string
	if !SAVE_LOCAL {
		// check first to see what hypertables exist that have compression, because we will get an error

		err = DB.Table("timescaledb_information.hypertables").Select(
			"hypertable_name",
		).Where("compression_enabled = TRUE").Scan(&existingHypertablesNames).Error

		if err != nil {
			writeLog(functionName, fmt.Sprintf(`Failed to get existing hypertables. Err: %s`, err), "ERROR")
			DB_VALID = false
			return err
		}

		for k, v := range conditionalMigrate {
			if !contains(existingHypertablesNames, k) {
				toMigrate = append(toMigrate, v)
			}
		}
	} else {
		toMigrate = append(toMigrate, &defs.SoldierState{})
		toMigrate = append(toMigrate, &defs.VehicleState{})
		toMigrate = append(toMigrate, &defs.FiredEvent{})
	}

	fmt.Printf("toMigrate: %s\n", toMigrate)

	err = DB.AutoMigrate(toMigrate...)
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Failed to migrate DB schema. Err: %s`, err), "ERROR")
		DB_VALID = false
		return err
	}

	if !SAVE_LOCAL {
		// if running TimescaleDB (Postgres), configure
		sqlDB, err = DB.DB()
		if err != nil {
			writeLog(functionName, fmt.Sprintf(`Failed to get DB.DB(). Err: %s`, err), "ERROR")
			DB_VALID = false
			return err
		}

		sqlDB.SetMaxOpenConns(30)

		// if running TimescaleDB, make sure that these tables, which are time-oriented and we want to maximize time based input, are partitioned in order for fast retrieval and compressed after 2 weeks to save disk space
		// https://docs.timescale.com/latest/using-timescaledb/hypertables

		hyperTables := map[string][]string{
			"soldier_states": {
				"time",
				"soldier_id",
				"capture_frame",
			},
			"vehicle_states": {
				"time",
				"vehicle_id",
				"capture_frame",
			},
			"fired_events": {
				"time",
				"soldier_id",
				"capture_frame",
			},
		}
		for k := range conditionalMigrate {
			if contains(existingHypertablesNames, k) {
				delete(hyperTables, k)
			}
		}
		err = validateHypertables(hyperTables)
		if err != nil {
			writeLog(functionName, `Failed to validate hypertables.`, "ERROR")
			DB_VALID = false
			return err
		}
	}

	writeLog(functionName, "DB initialized", "INFO")

	// init caches
	TEST_DATA_TIMEINC.Set(0)

	// CHANNEL LISTENER TO SAVE DATA
	go func() {
		// process channel data
		for v := range newSoldierChan {
			if !DB_VALID {
				return
			}

			obj, err := logNewSoldier(v)
			if err == nil {
				soldiersToWrite.Push([]defs.Soldier{obj})
			} else {
				writeLog(functionName, fmt.Sprintf(`Failed to log new soldier. Err: %s`, err), "ERROR")
			}
		}
	}()

	go func() {
		// process channel data
		for v := range newVehicleChan {
			if !DB_VALID {
				return
			}

			obj, err := logNewVehicle(v)
			if err == nil {
				vehiclesToWrite.Push([]defs.Vehicle{obj})
			} else {
				writeLog(functionName, fmt.Sprintf(`Failed to log new vehicle. Err: %s`, err), "ERROR")
			}
		}
	}()

	go func() {
		// process channel data
		for v := range newSoldierStateChan {
			if !DB_VALID {
				return
			}

			obj, err := logSoldierState(v)
			if err == nil {
				soldierStatesToWrite.Push([]defs.SoldierState{obj})
			} else {
				writeLog(functionName, fmt.Sprintf(`Failed to log soldier state. Err: %s`, err), "ERROR")
			}
		}
	}()

	go func() {
		// process channel data
		for v := range newVehicleStateChan {
			if !DB_VALID {
				return
			}

			obj, err := logVehicleState(v)
			if err == nil {
				vehicleStatesToWrite.Push([]defs.VehicleState{obj})
			} else {
				writeLog(functionName, fmt.Sprintf(`Failed to log vehicle state. Err: %s`, err), "ERROR")
			}
		}
	}()

	go func() {
		// process channel data
		for v := range newFiredEventChan {
			if !DB_VALID {
				return
			}

			obj, err := logFiredEvent(v)
			if err == nil {
				firedEventsToWrite.Push([]defs.FiredEvent{obj})
			} else {
				writeLog(functionName, fmt.Sprintf(`Failed to log fired event. Err: %s`, err), "ERROR")
			}
		}
	}()

	// start the DB Write goroutine
	go func() {
		for {
			if !DB_VALID {
				return
			}

			if PAUSE_INSERTS {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			var (
				tx         *gorm.DB = DB.Begin()
				txStart    time.Time
				writeStart time.Time = time.Now()
			)

			// write new soldiers
			if !soldiersToWrite.Empty() {
				txStart = time.Now()
				soldiersToWrite.Lock()
				err = tx.Create(&soldiersToWrite.Queue).Error
				soldiersToWrite.Unlock()
				if err != nil {
					writeLog(functionName, fmt.Sprintf(`Error creating soldiers: %v`, err), "ERROR")
				} else {
					writeLog(functionName, fmt.Sprintf(`Created %d soldiers in %s`, soldiersToWrite.Len(), time.Since(txStart)), "DEBUG")
				}
				soldiersToWrite.Clear()
			}

			// write soldier states
			if !soldierStatesToWrite.Empty() {
				txStart = time.Now()
				soldierStatesToWrite.Lock()
				err = tx.Create(&soldierStatesToWrite.Queue).Error
				soldierStatesToWrite.Unlock()
				if err != nil {
					writeLog(functionName, fmt.Sprintf(`Error creating soldier states: %v`, err), "ERROR")
					continue
				} else {
					writeLog(functionName, fmt.Sprintf(`Created %d soldier states in %s`, soldierStatesToWrite.Len(), time.Since(txStart)), "DEBUG")
				}
				soldierStatesToWrite.Clear()
			}

			// write new vehicles
			if !vehiclesToWrite.Empty() {
				txStart = time.Now()
				vehiclesToWrite.Lock()
				err = tx.Create(&vehiclesToWrite.Queue).Error
				vehiclesToWrite.Unlock()
				if err != nil {
					writeLog(functionName, fmt.Sprintf(`Error creating vehicles: %v`, err), "ERROR")
					continue
				} else {
					writeLog(functionName, fmt.Sprintf(`Created %d vehicles in %s`, vehiclesToWrite.Len(), time.Since(txStart)), "DEBUG")
				}
				vehiclesToWrite.Clear()
			}

			// write vehicle states
			if !vehicleStatesToWrite.Empty() {
				txStart = time.Now()
				vehicleStatesToWrite.Lock()
				err = tx.Create(&vehicleStatesToWrite.Queue).Error
				vehicleStatesToWrite.Unlock()
				if err != nil {
					writeLog(functionName, fmt.Sprintf(`Error creating vehicle states: %v`, err), "ERROR")
					continue
				} else {
					writeLog(functionName, fmt.Sprintf(`Created %d vehicle states in %s`, vehicleStatesToWrite.Len(), time.Since(txStart)), "DEBUG")
				}
				vehicleStatesToWrite.Clear()
			}

			// write fired events
			if !firedEventsToWrite.Empty() {
				txStart = time.Now()
				firedEventsToWrite.Lock()
				err = tx.Create(&firedEventsToWrite.Queue).Error
				firedEventsToWrite.Unlock()
				if err != nil {
					writeLog(functionName, fmt.Sprintf(`Error creating fired events: %v`, err), "ERROR")
					continue
				} else {
					writeLog(functionName, fmt.Sprintf(`Created %d fired events in %s`, firedEventsToWrite.Len(), time.Since(txStart)), "DEBUG")
				}
				firedEventsToWrite.Clear()
			}

			// commit transaction
			txStart = time.Now()
			err = tx.Commit().Error
			if err != nil {
				writeLog(functionName, fmt.Sprintf(`Error committing transaction: %v`, err), "ERROR")
				tx.Rollback()
			} else {
				writeLog(functionName, fmt.Sprintf(`Committed transaction in %s`, time.Since(txStart)), "DEBUG")
			}

			LAST_WRITE_DURATION = time.Since(writeStart)

			// sleep
			time.Sleep(1000 * time.Millisecond)

		}
	}()

	// goroutine to, every 10 seconds, pause insert execution and dump memory sqlite db to disk
	go func() {
		for {
			if !DB_VALID || !SAVE_LOCAL {
				return
			}

			time.Sleep(3 * time.Minute)

			// pause insert execution
			PAUSE_INSERTS = true

			// dump memory sqlite db to disk
			err = dumpMemoryDBToDisk()
			if err != nil {
				writeLog(functionName, fmt.Sprintf(`Error dumping memory db to disk: %v`, err), "ERROR")
			}

			// resume insert execution
			PAUSE_INSERTS = false
		}
	}()

	return nil
}

func validateHypertables(tables map[string][]string) (err error) {
	functionName := "validateHypertables"
	// HYPERTABLES

	all := []interface{}{}
	DB.Exec(`SELECT x.* FROM timescaledb_information.hypertables`).Scan(&all)
	for _, row := range all {
		writeLog(functionName, fmt.Sprintf(`hypertable row: %v`, row), "DEBUG")
	}

	// iterate through each provided table
	for table := range tables {
		hypertable := interface{}(nil)
		// see if table is already configured
		DB.Exec(`SELECT x.* FROM timescaledb_information.hypertables WHERE hypertable_name = ?`, table).Scan(&hypertable)
		if hypertable != nil {
			// table is already configured
			writeLog(functionName, fmt.Sprintf(`Table %s is already configured`, table), "INFO")
			continue
		}

		// if table doesn't exist, create it
		queryCreateHypertable := fmt.Sprintf(`
				SELECT create_hypertable('%s', 'time', chunk_time_interval => interval '1 day', if_not_exists => true);
			`, table)
		err = DB.Exec(queryCreateHypertable).Error
		if err != nil {
			writeLog(functionName, fmt.Sprintf(`Failed to create hypertable for %s. Err: %s`, table, err), "ERROR")
			return err
		} else {
			writeLog(functionName, fmt.Sprintf(`Created hypertable for %s`, table), "INFO")
		}

		// set compression
		queryCompressHypertable := fmt.Sprintf(`
				ALTER TABLE %s SET (
					timescaledb.compress,
					timescaledb.compress_segmentby = ?);
			`, table)
		err = DB.Exec(
			queryCompressHypertable,
			strings.Join(tables[table], ","),
		).Error
		if err != nil {
			writeLog(functionName, fmt.Sprintf(`Failed to enable compression for %s. Err: %s`, table, err), "ERROR")
			return err
		} else {
			writeLog(functionName, fmt.Sprintf(`Enabled hypertable compression for %s`, table), "INFO")
		}

		// set compress_after
		queryCompressAfterHypertable := fmt.Sprintf(`
				SELECT add_compression_policy(
					'%s',
					compress_after => interval '14 day');
			`, table)
		err = DB.Exec(queryCompressAfterHypertable).Error
		if err != nil {
			writeLog(functionName, fmt.Sprintf(`Failed to set compress_after for %s. Err: %s`, table, err), "ERROR")
			return err
		} else {
			writeLog(functionName, fmt.Sprintf(`Set compress_after for %s`, table), "INFO")
		}
	}
	return nil
}

// WORLDS AND MISSIONS
var CurrentWorld defs.World
var CurrentMission defs.Mission

// logNewMission logs a new mission to the database and associates the world it's played on
func logNewMission(data []string) (err error) {
	functionName := ":NEW:MISSION:"

	world := defs.World{}
	mission := defs.Mission{}
	// unmarshal data[0]
	err = json.Unmarshal([]byte(data[0]), &world)
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Error unmarshalling world data: %v`, err), "ERROR")
		return err
	}

	// unmarshal data[1]
	err = json.Unmarshal([]byte(data[1]), &mission)
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Error unmarshalling mission data: %v`, err), "ERROR")
		return err
	}

	mission.StartTime = time.Now()

	// check if world exists
	err = DB.Where("world_name = ?", world.WorldName).First(&world).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		writeLog(functionName, fmt.Sprintf(`Error checking if world exists: %v`, err), "ERROR")
		return err
	}
	if world.ID == 0 {
		// world does not exist, create it
		err = DB.Create(&world).Error
		if err != nil {
			writeLog(functionName, fmt.Sprintf(`Error creating world: %v`, err), "ERROR")
			return err
		}
	}

	// always write new mission
	mission.World = world
	err = DB.Create(&mission).Error
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Error creating mission: %v`, err), "ERROR")
		return err
	}

	// write to log
	writeLog(functionName, fmt.Sprintf(`New mission logged: %s`, mission.MissionName), "INFO")

	// set current world and mission
	CurrentWorld = world
	CurrentMission = mission

	return nil
}

// (UNITS) AND VEHICLES

// logNewSoldier logs a new soldier to the database
func logNewSoldier(data []string) (soldier defs.Soldier, err error) {
	functionName := ":NEW:SOLDIER:"
	// check if DB is valid
	if !DB_VALID {
		return
	}

	// fix received data
	for i, v := range data {
		data[i] = fixEscapeQuotes(trimQuotes(v))
	}

	// get frame
	frameStr := data[0]
	capframe, err := strconv.ParseInt(frameStr, 10, 64)
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Error converting capture frame to int: %s`, err), "ERROR")
		return soldier, err
	}

	// parse array
	soldier.MissionID = CurrentMission.ID
	soldier.JoinTime = time.Now()
	soldier.JoinFrame = uint(capframe)
	ocapId, err := strconv.ParseUint(data[1], 10, 64)
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Error converting ocapId to uint: %v`, err), "ERROR")
		return soldier, err
	}
	soldier.OcapID = uint16(ocapId)
	soldier.UnitName = data[2]
	soldier.GroupID = data[3]
	soldier.Side = data[4]
	soldier.IsPlayer, err = strconv.ParseBool(data[5])
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Error converting isPlayer to bool: %v`, err), "ERROR")
		return soldier, err
	}
	soldier.RoleDescription = data[6]
	// player uid
	soldier.PlayerUID = data[7]

	return soldier, nil

	// log to database
	// err = tx.Create(&soldier).Error
	// if err != nil {
	// 	writeLog(functionName, fmt.Sprintf(`Error creating soldier: %v -- %v`, soldier, err), "ERROR")
	// 	return
	// }

}

// logSoldierState logs a SoldierState state to the database
func logSoldierState(data []string) (soldierState defs.SoldierState, err error) {
	functionName := ":NEW:SOLDIER:STATE:"
	// check if DB is valid
	if !DB_VALID {
		return soldierState, nil
	}

	// fix received data
	for i, v := range data {
		data[i] = fixEscapeQuotes(trimQuotes(v))
	}

	frameStr := data[8]
	capframe, err := strconv.ParseInt(frameStr, 10, 64)
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Error converting capture frame to int: %s`, err), "ERROR")
		return soldierState, err
	}
	soldierState.CaptureFrame = uint(capframe)

	// parse data in array
	ocapId, err := strconv.ParseUint(data[0], 10, 64)
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Error converting ocapId to uint: %v`, err), "ERROR")
		return soldierState, err
	}

	// try and find soldier in DB to associate
	soldierId := uint(0)
	err = DB.Model(&defs.Soldier{}).Select("id").Order("join_time DESC").Where("ocap_id = ?", uint16(ocapId)).First(&soldierId).Error
	if err != nil {
		json, _ := json.Marshal(data)
		writeLog(functionName, fmt.Sprintf("Error finding soldier in DB:\n%s\n%v", json, err), "ERROR")
		return soldierState, err
	}
	soldierState.SoldierID = soldierId

	// when loading test data, set time to random offset
	if TEST_DATA {
		newTime := time.Now().Add(time.Duration(TEST_DATA_TIMEINC.Value()) * time.Second)
		TEST_DATA_TIMEINC.Inc()
		soldierState.Time = newTime
	} else {
		soldierState.Time = time.Now()
	}

	// parse pos from an arma string
	pos := data[1]
	pos = strings.TrimPrefix(pos, "[")
	pos = strings.TrimSuffix(pos, "]")
	point, elev, err := defs.GPSFromString(pos, 3857, SAVE_LOCAL)
	if err != nil {
		json, _ := json.Marshal(data)
		writeLog(functionName, fmt.Sprintf("Error converting position to Point:\n%s\n%v", json, err), "ERROR")
		return soldierState, err
	}
	// fmt.Println(point.ToString())
	soldierState.Position = point
	soldierState.ElevationASL = float32(elev)

	// bearing
	bearing, _ := strconv.Atoi(data[2])
	soldierState.Bearing = uint16(bearing)
	// lifestate
	lifeState, _ := strconv.Atoi(data[3])
	soldierState.Lifestate = uint8(lifeState)
	// in vehicle
	soldierState.InVehicle, _ = strconv.ParseBool(data[4])
	// name
	soldierState.UnitName = data[5]
	// is player
	isPlayer, err := strconv.ParseBool(data[6])
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Error converting isPlayer to bool: %v`, err), "ERROR")
		return soldierState, err
	}
	soldierState.IsPlayer = isPlayer
	// current role
	soldierState.CurrentRole = data[7]

	// if len > 8, we're running ace medical and have more data
	if len(data) > 8 {
		// has stable vitals
		hasStableVitals, _ := strconv.ParseBool(data[8])
		soldierState.HasStableVitals = hasStableVitals
		// is dragged/carried
		isDraggedCarried, _ := strconv.ParseBool(data[9])
		soldierState.IsDraggedCarried = isDraggedCarried
	}

	return soldierState, nil
}

func logNewVehicle(data []string) (vehicle defs.Vehicle, err error) {
	functionName := ":NEW:VEHICLE:"
	// check if DB is valid
	if !DB_VALID {
		return vehicle, nil
	}

	// fix received data
	for i, v := range data {
		data[i] = fixEscapeQuotes(trimQuotes(v))
	}

	// get frame
	frameStr := data[0]
	capframe, err := strconv.ParseInt(frameStr, 10, 64)
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Error converting capture frame to int: %s`, err), "ERROR")
		return vehicle, err
	}

	// parse array
	vehicle.MissionID = CurrentMission.ID
	vehicle.JoinTime = time.Now()
	vehicle.JoinFrame = uint(capframe)
	ocapId, err := strconv.ParseUint(data[1], 10, 64)
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Error converting ocapId to uint: %v`, err), "ERROR")
		return vehicle, err
	}
	vehicle.OcapID = uint16(ocapId)
	vehicle.VehicleClass = data[2]
	vehicle.DisplayName = data[3]

	return vehicle, nil
}

func logVehicleState(data []string) (vehicleState defs.VehicleState, err error) {
	functionName := ":NEW:VEHICLE:STATE:"
	// check if DB is valid
	if !DB_VALID {
		return vehicleState, nil
	}

	// fix received data
	for i, v := range data {
		data[i] = fixEscapeQuotes(trimQuotes(v))
	}

	// get frame
	frameStr := data[len(data)-1]
	capframe, err := strconv.ParseInt(frameStr, 10, 64)
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Error converting capture frame to int: %s`, err), "ERROR")
		return vehicleState, err
	}
	vehicleState.CaptureFrame = uint(capframe)

	// parse data in array
	ocapId, err := strconv.ParseUint(data[0], 10, 64)
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Error converting ocapId to uint: %v`, err), "ERROR")
		return vehicleState, err
	}

	// try and find vehicle in DB to associate
	vehicleId := uint(0)
	err = DB.Model(&defs.Vehicle{}).Select("id").Order("join_time DESC").Where("ocap_id = ?", uint16(ocapId)).First(&vehicleId).Error
	if err != nil {
		json, _ := json.Marshal(data)
		writeLog(functionName, fmt.Sprintf("Error finding vehicle in DB:\n%s\n%v", json, err), "ERROR")
		return vehicleState, err
	}
	vehicleState.VehicleID = vehicleId

	// when loading test data, set time to random offset
	if TEST_DATA {
		newTime := time.Now().Add(time.Duration(TEST_DATA_TIMEINC.Value()) * time.Second)
		TEST_DATA_TIMEINC.Inc()
		vehicleState.Time = newTime
	} else {
		vehicleState.Time = time.Now()
	}

	// parse pos from an arma string
	pos := data[1]
	pos = strings.TrimPrefix(pos, "[")
	pos = strings.TrimSuffix(pos, "]")
	point, elev, err := defs.GPSFromString(pos, 3857, SAVE_LOCAL)
	if err != nil {
		json, _ := json.Marshal(data)
		writeLog(functionName, fmt.Sprintf("Error converting position to Point:\n%s\n%v", json, err), "ERROR")
		return vehicleState, err
	}
	// fmt.Println(point.ToString())
	vehicleState.Position = point
	vehicleState.ElevationASL = float32(elev)

	// bearing
	bearing, _ := strconv.Atoi(data[2])
	vehicleState.Bearing = uint16(bearing)
	// is alive
	isAlive, _ := strconv.ParseBool(data[3])
	vehicleState.IsAlive = isAlive
	// parse crew, which is an array of ocap ids of soldiers
	crew := data[4]
	crew = strings.TrimPrefix(crew, "[")
	crew = strings.TrimSuffix(crew, "]")
	var crewIds []string = strings.Split(crew, ",")
	var crewIdsUint []uint16
	for _, v := range crewIds {
		crewId, err := strconv.ParseUint(v, 10, 16)
		if err != nil {
			writeLog(functionName, fmt.Sprintf(`Error converting crewId to uint: %v`, err), "ERROR")
			return vehicleState, err
		}
		crewIdsUint = append(crewIdsUint, uint16(crewId))
	}

	vehicleState.Crew, err = json.Marshal(crewIdsUint)
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Error converting crew to json: %v`, err), "ERROR")
		return
	}

	return vehicleState, nil
}

// FIRED EVENTS
func logFiredEvent(data []string) (firedEvent defs.FiredEvent, err error) {
	functionName := ":FIRED:"
	// check if DB is valid
	if !DB_VALID {
		return firedEvent, nil
	}

	// fix received data
	for i, v := range data {
		data[i] = fixEscapeQuotes(trimQuotes(v))
	}

	// get frame
	frameStr := data[1]
	capframe, err := strconv.ParseInt(frameStr, 10, 64)
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Error converting capture frame to int: %s`, err), "ERROR")
		return firedEvent, err
	}
	firedEvent.CaptureFrame = uint(capframe)

	// parse data in array
	ocapId, err := strconv.ParseUint(data[0], 10, 64)
	if err != nil {
		writeLog(functionName, fmt.Sprintf(`Error converting ocapId to uint: %v`, err), "ERROR")
		return firedEvent, err
	}

	// try and find soldier in DB to associate
	soldierId := uint(0)
	err = DB.Model(&defs.Soldier{}).Select("id").Order("join_time DESC").Where("ocap_id = ?", uint16(ocapId)).First(&soldierId).Error
	if err != nil {
		json, _ := json.Marshal(data)
		writeLog(functionName, fmt.Sprintf("Error finding soldier in DB:\n%s\n%v", json, err), "ERROR")
		return firedEvent, err
	}
	firedEvent.SoldierID = soldierId

	// when loading test data, set time to random offset
	if TEST_DATA {
		newTime := time.Now().Add(time.Duration(TEST_DATA_TIMEINC.Value()) * time.Second)
		TEST_DATA_TIMEINC.Inc()
		firedEvent.Time = newTime
	} else {
		firedEvent.Time = time.Now()
	}

	// parse BULLET START POS from an arma string
	startpos := data[2]
	startpos = strings.TrimPrefix(startpos, "[")
	startpos = strings.TrimSuffix(startpos, "]")
	startpoint, startelev, err := defs.GPSFromString(startpos, 3857, SAVE_LOCAL)
	if err != nil {
		json, _ := json.Marshal(data)
		writeLog(functionName, fmt.Sprintf("Error converting position to Point:\n%s\n%v", json, err), "ERROR")
		return firedEvent, err
	}
	firedEvent.StartPosition = startpoint
	firedEvent.StartElevationASL = float32(startelev)

	// parse BULLET END POS from an arma string
	endpos := data[3]
	endpos = strings.TrimPrefix(endpos, "[")
	endpos = strings.TrimSuffix(endpos, "]")
	endpoint, endelev, err := defs.GPSFromString(endpos, 3857, SAVE_LOCAL)
	if err != nil {
		json, _ := json.Marshal(data)
		writeLog(functionName, fmt.Sprintf("Error converting position to Point:\n%s\n%v", json, err), "ERROR")
		return firedEvent, err
	}
	firedEvent.EndPosition = endpoint
	firedEvent.EndElevationASL = float32(endelev)

	// weapon name
	firedEvent.Weapon = data[4]
	// magazine name
	firedEvent.Magazine = data[5]
	// firing mode
	firedEvent.FiringMode = data[6]

	return firedEvent, nil
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

// function to process events of different kinds
func processEvent(data []string) {
	event := data[1]
	switch event {
	case "connected":
		object := defs.EventPlayerConnect{}
		captureFrame, _ := strconv.Atoi(data[0])
		object.CaptureFrame = uint32(captureFrame)
		object.ProfileName = data[2]
		object.PlayerUID = data[3]
		object.MissionID = CurrentMission.ID

		// write
		DB.Create(&object)
	case "disconnected":
		object := defs.EventPlayerDisconnect{}
		captureFrame, _ := strconv.Atoi(data[0])
		object.CaptureFrame = uint32(captureFrame)
		object.ProfileName = data[2]
		object.PlayerUID = data[3]
		object.MissionID = CurrentMission.ID

		// write
		DB.Create(&object)
	default:
		writeLog("processEvent", fmt.Sprintf(`Unknown event type: %s`, event), "ERROR")
	}
}

///////////////////////
// EXPORTED FUNCTIONS //
///////////////////////

func runExtensionCallback(name *C.char, function *C.char, data *C.char) C.int {
	return C.runExtensionCallback(extensionCallbackFnc, name, function, data)
}

//export goRVExtensionVersion
func goRVExtensionVersion(output *C.char, outputsize C.size_t) {
	result := C.CString(EXTENSION_VERSION)
	defer C.free(unsafe.Pointer(result))
	var size = C.strlen(result) + 1
	if size > outputsize {
		size = outputsize
	}
	C.memmove(unsafe.Pointer(output), unsafe.Pointer(result), size)
}

//export goRVExtensionArgs
func goRVExtensionArgs(output *C.char, outputsize C.size_t, input *C.char, argv **C.char, argc C.int) {
	var offset = unsafe.Sizeof(uintptr(0))
	var out []string
	for index := C.int(0); index < argc; index++ {
		out = append(out, C.GoString(*argv))
		argv = (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(argv)) + offset))
	}

	// temp := fmt.Sprintf("Function: %s nb params: %d params: %s!", C.GoString(input), argc, out)
	temp := fmt.Sprintf("Function: %s nb params: %d", C.GoString(input), argc)

	switch C.GoString(input) {
	case "getDB":
		// callExtension ["logAttendance", [_hash] call CBA_fnc_encodeJSON]];
		err := getDB()
		if err != nil {
			temp = fmt.Sprintf(
				`[1, "Error getting DB: %s"]`,
				strings.Replace(err.Error(), `"`, `""`, -1),
			)
		} else {
			temp = `[0, "DB initialized"]`
		}
	case ":NEW:MISSION:":
		err := logNewMission(out)
		if err != nil {
			temp = fmt.Sprintf(
				`[1, "%s"]`,
				strings.Replace(err.Error(), `"`, `""`, -1),
			)
		} else {
			temp = `[0, "Mission logged"]`
		}
	case ":NEW:SOLDIER:":
		newSoldierChan <- out
		temp = `[0, "Logging unit"]`
	case ":NEW:SOLDIER:STATE:":
		newSoldierStateChan <- out
		temp = `[0, "Logging unit state"]`
	case ":NEW:VEHICLE:":
		newVehicleChan <- out
		temp = `[0, "Logging vehicle"]`
	case ":NEW:VEHICLE:STATE:":
		newVehicleStateChan <- out
		temp = `[0, "Logging vehicle state"]`
	case ":FIRED:":
		newFiredEventChan <- out
	case ":EVENT:":
		{
			go processEvent(out)
		}
	}

	// Return a result to Arma
	result := C.CString(temp)
	defer C.free(unsafe.Pointer(result))
	var size = C.strlen(result) + 1
	if size > outputsize {
		size = outputsize
	}

	C.memmove(unsafe.Pointer(output), unsafe.Pointer(result), size)
}

func callBackExample() {
	name := C.CString("arma")
	defer C.free(unsafe.Pointer(name))
	function := C.CString("funcToExecute")
	defer C.free(unsafe.Pointer(function))
	// Make a callback to Arma
	for i := 0; i < 3; i++ {
		time.Sleep(2 * time.Second)
		param := C.CString(fmt.Sprintf("Loop: %d", i))
		defer C.free(unsafe.Pointer(param))
		runExtensionCallback(name, function, param)
	}
}

func getTimestamp() string {
	// get the current unix timestamp in nanoseconds
	// return time.Now().Local().Unix()
	return time.Now().Format("2006-01-02 15:04:05")
}

func trimQuotes(s string) string {
	// trim the start and end quotes from a string
	return strings.Trim(s, `"`)
}

func fixEscapeQuotes(s string) string {
	// fix the escape quotes in a string
	return strings.Replace(s, `""`, `"`, -1)
}

func writeLog(functionName string, data string, level string) {
	// get calling function & line
	_, file, line, _ := runtime.Caller(1)

	if activeSettings.Debug && level == "DEBUG" {
		log.Printf(`%s:%d:%s [%s] %s`, path.Base(file), line, functionName, level, data)
	} else if level != "DEBUG" {
		log.Printf(`%s:%d:%s [%s] %s`, path.Base(file), line, functionName, level, data)
	}

	if extensionCallbackFnc != nil {
		// replace double quotes with 2 double quotes
		escapedData := strings.Replace(data, `"`, `""`, -1)
		// do the same for single quotes
		escapedData = strings.Replace(escapedData, `'`, `'`, -1)
		a3Message := fmt.Sprintf(`["%s", "%s"]`, escapedData, level)

		statusName := C.CString(EXTENSION)
		defer C.free(unsafe.Pointer(statusName))
		statusFunction := C.CString(functionName)
		defer C.free(unsafe.Pointer(statusFunction))
		statusParam := C.CString(a3Message)
		defer C.free(unsafe.Pointer(statusParam))
		runExtensionCallback(statusName, statusFunction, statusParam)
	}
}

//export goRVExtension
func goRVExtension(output *C.char, outputsize C.size_t, input *C.char) {

	var temp string

	// logLine("goRVExtension", fmt.Sprintf(`["Input: %s",  "DEBUG"]`, C.GoString(input)), true)

	switch C.GoString(input) {
	case "version":
		temp = EXTENSION_VERSION
	case "getDir":
		temp = getDir()

	default:
		temp = fmt.Sprintf(`["%s"]`, "Unknown Function")
	}

	result := C.CString(temp)
	defer C.free(unsafe.Pointer(result))
	var size = C.strlen(result) + 1
	if size > outputsize {
		size = outputsize
	}

	C.memmove(unsafe.Pointer(output), unsafe.Pointer(result), size)
	// return
}

//export goRVExtensionRegisterCallback
func goRVExtensionRegisterCallback(fnc C.extensionCallback) {
	extensionCallbackFnc = fnc
}

func populateDemoData() {
	if !DB_VALID {
		return
	}

	// declare test size counts
	var (
		numMissions                int = 2
		missionDuration            int = 60 * 30                                          // s * value (m) = total (s)
		numUnitsPerMission         int = 50                                               // num units per mission
		numUnits                   int = numMissions * numUnitsPerMission                 // num missions * num unique units
		numSoldiers                int = int(math.Ceil(float64(numUnits) * float64(0.8))) // numUnits / 3
		numFiredEventsPerSoldier   int = 2700
		numSoldierStatesPerSoldier int = missionDuration                                  // missionDuration (1s frames)
		numVehicles                int = int(math.Ceil(float64(numUnits) * float64(0.2))) // numUnits / 3
		numVehicleStates           int = numVehicles * missionDuration                    // numVehicles * missionDuration (1s frames)

		sides []string = []string{
			"WEST",
			"EAST",
			"GUER",
			"CIV",
		}

		vehicleClassnames []string = []string{
			"B_MRAP_01_F",
			"B_MRAP_01_gmg_F",
			"B_MRAP_01_hmg_F",
			"B_G_Offroad_01_armed_F",
			"B_G_Offroad_01_AT_F",
			"B_G_Offroad_01_F",
			"B_G_Offroad_01_repair_F",
			"B_APC_Wheeled_01_cannon_F",
			"B_APC_Tracked_01_AA_F",
			"B_APC_Tracked_01_CRV_F",
			"B_APC_Tracked_01_rcws_F",
			"B_APC_Tracked_01_CRV_F",
		}

		roles []string = []string{
			"Rifleman",
			"Team Leader",
			"Auto Rifleman",
			"Assistant Auto Rifleman",
			"Grenadier",
			"Machine Gunner",
			"Assistant Machine Gunner",
			"Medic",
			"Engineer",
			"Explosive Specialist",
			"Rifleman (AT)",
			"Rifleman (AA)",
			"Officer",
		}

		weapons []string = []string{
			"Katiba",
			"MXC 6.5 mm",
			"MX 6.5 mm",
			"MX SW 6.5 mm",
			"MXM 6.5 mm",
			"SPAR-16 5.56 mm",
			"SPAR-16S 5.56 mm",
			"SPAR-17 7.62 mm",
			"TAR-21 5.56 mm",
			"TRG-21 5.56 mm",
			"TRG-20 5.56 mm",
			"TRG-21 EGLM 5.56 mm",
			"CAR-95 5.8 mm",
			"CAR-95-1 5.8 mm",
			"CAR-95 GL 5.8 mm",
		}

		magazines []string = []string{
			"30rnd 6.5 mm Caseless Mag",
			"30rnd 6.5 mm Caseless Mag Tracer",
			"100rnd 6.5 mm Caseless Mag",
			"100rnd 6.5 mm Caseless Mag Tracer",
			"200rnd 6.5 mm Caseless Mag",
			"200rnd 6.5 mm Caseless Mag Tracer",
			"30rnd 5.56 mm STANAG",
			"30rnd 5.56 mm STANAG Tracer (Yellow)",
			"30rnd 5.56 mm STANAG Tracer (Red)",
			"30rnd 5.56 mm STANAG Tracer (Green)",
		}

		firemodes []string = []string{
			"Single",
			"FullAuto",
			"Burst3",
			"Burst5",
		}

		worldNames []string = []string{
			"Altis",
			"Stratis",
			"VR",
			"Bootcamp_ACR",
			"Malden",
			"ProvingGrounds_PMC",
			"Shapur_BAF",
			"Sara",
			"Sara_dbe1",
			"SaraLite",
			"Woodland_ACR",
			"Chernarus",
			"Desert_E",
			"Desert_Island",
			"Intro",
			"Desert2",
		}
	)

	// start a goroutine that will output channel lengths every second
	go func() {
		for {
			fmt.Printf("PENDING: Soldiers: %d, SoldierStates: %d, Vehicles: %d, VehicleStates: %d\n",
				len(newSoldierChan),
				len(newSoldierStateChan),
				len(newVehicleChan),
				len(newVehicleStateChan),
			)
			fmt.Printf("LAST WRITE TOOK: %s\n", LAST_WRITE_DURATION)
			time.Sleep(1000 * time.Millisecond)
		}
	}()

	// write worlds
	missionsStart := time.Now()
	for i := 0; i < numMissions; i++ {
		data := make([]string, 2)

		// WORLD CONTEXT SENT AS JSON IN DATA[0]
		// ["author", _author],
		// ["workshopID", _workshopID],
		// ["displayName", _name],
		// ["worldName", toLower worldName],
		// ["worldNameOriginal", worldName],
		// ["worldSize", getNumber(configFile >> "CfgWorlds" >> worldName >> "worldSize")],
		// ["latitude", getNumber(configFile >> "CfgWorlds" >> worldName >> "latitude")],
		// ["longitude", getNumber(configFile >> "CfgWorlds" >> worldName >> "longitude")]

		// pick random name from list
		worldNameOriginal := worldNames[rand.Intn(len(worldNames))]
		// displayname, replace underscores with spaces
		displayName := strings.Replace(worldNameOriginal, "_", " ", -1)
		// worldName, lowercase
		worldName := strings.ToLower(worldNameOriginal)

		worldData := map[string]interface{}{
			"author":            "Demo Author",
			"workshopID":        "123456789",
			"displayName":       displayName,
			"worldName":         worldName,
			"worldNameOriginal": worldNameOriginal,
			"worldSize":         10240,
			"latitude":          0.0,
			"longitude":         0.0,
		}
		worldDataJSON, err := json.Marshal(worldData)
		if err != nil {
			fmt.Println(err)
		}
		data[0] = string(worldDataJSON)

		// MISSION CONTEXT SENT AS STRING IN DATA[1]
		// ["missionName", missionName],
		// ["briefingName", briefingName],
		// ["missionNameSource", missionNameSource],
		// ["onLoadName", getMissionConfigValue ["onLoadName", ""]],
		// ["author", getMissionConfigValue ["author", ""]],
		// ["serverName", serverName],
		// ["serverProfile", profileName],
		// ["missionStart", "0"],
		// ["worldName", toLower worldName],
		// ["tag", EGVAR(settings,saveTag)]
		missionData := map[string]interface{}{
			"missionName":       fmt.Sprintf("Demo Mission %d", i),
			"briefingName":      fmt.Sprintf("Demo Briefing %d", i),
			"missionNameSource": fmt.Sprintf("Demo Mission %d", i),
			"onLoadName":        "",
			"author":            "Demo Author",
			"serverName":        "Demo Server",
			"serverProfile":     "Demo Profile",
			"missionStart":      nil, // random time
			"worldName":         fmt.Sprintf("demo_world_%d", i),
			"tag":               "Demo Tag",
		}
		missionDataJSON, err := json.Marshal(missionData)
		if err != nil {
			fmt.Println(err)
		}
		data[1] = string(missionDataJSON)

		err = logNewMission(data)
		if err != nil {
			fmt.Println(err)
		}

		time.Sleep(500 * time.Millisecond)
	}
	missionsEnd := time.Now()
	missionsElapsed := missionsEnd.Sub(missionsStart)
	fmt.Printf("Sent %d missions in %s\n", numMissions, missionsElapsed)

	// write soldiers, now that our missions exist and the channels have been created
	soldiersStart := time.Now()
	idCounter := 1
	for i := 0; i < numSoldiers; i++ {

		// pick random mission
		DB.Model(&defs.Mission{}).Order("RANDOM()").Limit(1).First(&CurrentMission)

		// soldier := Soldier{
		// 	MissionID: mission.ID,
		// 	// jointime is mission.StartTime + random time between 0 and missionDuration (seconds)
		// 	JoinTime: mission.StartTime.Add(time.Duration(rand.Intn(missionDuration)) * time.Second),
		// 	// joinframe is random time between 0 and missionDuration seconds
		// 	JoinFrame: uint(rand.Intn(missionDuration)),
		// 	// OcapID is random number between 1 and numUnits
		// 	OcapID: uint16(rand.Intn(numUnits) + 1),
		// 	// UnitName is random string
		// 	UnitName: fmt.Sprintf("Demo Unit %d", i),
		// 	// GroupID is random string
		// 	GroupID: fmt.Sprintf("Demo Group %d", i),
		// 	// Side is random string from sides
		// 	Side: sides[rand.Intn(len(sides))],
		// 	// isPlayer is random bool
		// 	IsPlayer: rand.Intn(2) == 1,
		// 	// RoleDescription is random string from roles
		// 	RoleDescription: roles[rand.Intn(len(roles))],
		// }

		// these will be sent as an array
		frame := strconv.FormatInt(int64(rand.Intn(missionDuration)), 10)
		soldier := []string{
			frame,
			strconv.FormatInt(int64(idCounter), 10),
			fmt.Sprintf("Demo Unit %d", i),
			fmt.Sprintf("Demo Group %d", i),
			sides[rand.Intn(len(sides))],
			strconv.FormatBool(rand.Intn(2) == 1),
			roles[rand.Intn(len(roles))],
			// random player uid
			strconv.FormatInt(int64(rand.Intn(1000000000)), 10),
		}

		newSoldierChan <- soldier

		// sleep to ensure soldier is written
		time.Sleep(2000 * time.Millisecond)

		// write soldier states
		soldierStatesStart := time.Now()
		var randomPos [3]float64 = [3]float64{rand.Float64() * 30720, rand.Float64() * 30720, rand.Float64() * 30720}
		var randomDir float64 = rand.Float64() * 360
		var randomLifestate int = rand.Intn(3)
		var currentRole string = roles[rand.Intn(len(roles))]
		for i := 0; i < numSoldierStatesPerSoldier; i++ {

			// determine xy transform to translate pos in randomDir
			var xyTransform [2]float64 = [2]float64{math.Cos(randomDir), math.Sin(randomDir)}
			// adjust random pos by random + or - 4 in xyTransform direction
			randomPos[0] += xyTransform[0] + (rand.Float64() * 8) - 4
			randomPos[1] += xyTransform[1] + (rand.Float64() * 8) - 4
			randomPos[2] += (rand.Float64() * 8) - 4

			// adjust random dir by random + or - 4
			randomDir += (rand.Float64() * 8) - 4

			// 10% chance of setting random lifestate
			if rand.Intn(10) == 0 {
				randomLifestate = rand.Intn(3)
			}

			// 5% chance of setting random role
			if rand.Intn(20) == 0 {
				currentRole = roles[rand.Intn(len(roles))]
			}

			soldierState := []string{
				// random id
				strconv.FormatInt(int64(idCounter), 10),
				// random pos
				fmt.Sprintf("[%f,%f,%f]", randomPos[0], randomPos[1], randomPos[2]),
				// random dir
				strconv.FormatFloat(randomDir, 'f', 6, 64),
				// random lifestate (0 to 2)
				strconv.FormatInt(int64(randomLifestate), 10),
				// random inVehicle bool
				strconv.FormatBool(rand.Intn(2) == 1),
				// random name
				fmt.Sprintf("Demo Unit %d", i),
				// random isPlayer bool
				strconv.FormatBool(rand.Intn(2) == 1),
				// random role
				currentRole,
				// random capture frame
				strconv.FormatInt(int64(rand.Intn(missionDuration)), 10),
				// hasStableVitals 0 or 1
				strconv.FormatInt(int64(rand.Intn(2)), 10),
				// is being dragged/carried 0 or 1
				strconv.FormatInt(int64(rand.Intn(2)), 10),
			}

			newSoldierStateChan <- soldierState
		}
		fmt.Printf("Sent %d soldier states in %s\n", numSoldierStatesPerSoldier, time.Since(soldierStatesStart))

		idCounter++
	}
	soldiersEnd := time.Now()
	soldiersElapsed := soldiersEnd.Sub(soldiersStart)
	fmt.Printf("Sent %d soldiers in %s\n", numSoldiers, soldiersElapsed)

	// allow 5 seconds for all soldiers to be written
	fmt.Println("Waiting for soldiers to be written...")
	time.Sleep(5 * time.Second)
	fmt.Println("Done waiting.")

	// write vehicles
	vehiclesStart := time.Now()
	vehicleIdCounter := 1
	for i := 0; i < numVehicles; i++ {
		// get random mission
		DB.Model(&defs.Mission{}).Order("RANDOM()").Limit(1).First(&CurrentMission)

		vehicle := []string{
			// random frame
			strconv.FormatInt(int64(rand.Intn(missionDuration)), 10),
			// random id
			strconv.FormatInt(int64(vehicleIdCounter), 10),
			// random classname
			vehicleClassnames[rand.Intn(len(vehicleClassnames))],
			// random display name
			fmt.Sprintf("Demo Vehicle %d", i),
		}

		vehicleIdCounter += 1
		newVehicleChan <- vehicle
	}

	fmt.Printf("Sent %d vehicles in %s\n", numVehicles, time.Since(vehiclesStart))

	// allow 5 seconds for all vehicles to be written
	fmt.Println("Waiting for vehicles to be written...")
	time.Sleep(5 * time.Second)
	fmt.Println("Done waiting.")

	// send vehicle states
	vehicleStatesStart := time.Now()
	for i := 0; i < numVehicleStates; i++ {
		// get random mission
		DB.Model(&defs.Mission{}).Order("RANDOM()").Limit(1).First(&CurrentMission)

		vehicleState := []string{
			// random id
			strconv.FormatInt(int64(rand.Intn(numVehicles)+1), 10),
			// random pos
			fmt.Sprintf("[%f,%f,%f]", rand.Float64()*30720+1, rand.Float64()*30720+1, rand.Float64()*30720+1),
			// random dir
			fmt.Sprintf("%d", rand.Intn(360)),
			// random isAlive bool
			strconv.FormatBool(rand.Intn(2) == 1),
			// random crew (array of soldiers)
			fmt.Sprintf("[%d,%d,%d]", rand.Intn(numUnitsPerMission)+1, rand.Intn(numUnitsPerMission)+1, rand.Intn(numUnitsPerMission)+1),
			// random frame
			strconv.FormatInt(int64(rand.Intn(missionDuration)), 10),
		}

		newVehicleStateChan <- vehicleState
	}

	fmt.Printf("Sent %d vehicle states in %s\n", numVehicleStates, time.Since(vehicleStatesStart))

	// allow 5 seconds for all vehicle states to be written
	fmt.Println("Waiting for vehicle states to be written...")
	time.Sleep(5 * time.Second)
	fmt.Println("Done waiting.")

	// demo fired events, X per soldier per mission
	firedEventsStart := time.Now()
	for i := 0; i < numSoldiers*numFiredEventsPerSoldier; i++ {
		// get random mission
		DB.Model(&defs.Mission{}).Order("RANDOM()").Limit(1).First(&CurrentMission)

		var randomStartPos []float64 = []float64{rand.Float64()*30720 + 1, rand.Float64()*30720 + 1, rand.Float64()*30720 + 1}
		// generate random end pos within 200 m
		var randomEndPos []float64
		for j := 0; j < 3; j++ {
			randomEndPos = append(randomEndPos, randomStartPos[j]+rand.Float64()*400-200)
		}

		firedEvent := []string{
			// random soldier id
			strconv.FormatInt(int64(rand.Intn(numSoldiers)+1), 10),
			// random frame
			strconv.FormatInt(int64(rand.Intn(missionDuration)), 10),
			// random start pos
			fmt.Sprintf("[%f,%f,%f]", randomStartPos[0], randomStartPos[1], randomStartPos[2]),
			// random end pos within 200m of start pos
			fmt.Sprintf("[%f,%f,%f]", randomEndPos[0], randomEndPos[1], randomEndPos[2]),
			// random weapon
			weapons[rand.Intn(len(weapons))],
			// random magazine
			magazines[rand.Intn(len(magazines))],
			// random firemode
			firemodes[rand.Intn(len(firemodes))],
		}

		newFiredEventChan <- firedEvent
	}

	fmt.Printf("Sent %d fired events in %s\n", numSoldiers*numFiredEventsPerSoldier, time.Since(firedEventsStart))

	fmt.Println("Demo data populated. Press enter to exit.")
	fmt.Scanln()
}

func getDemoJSON(missionIds []string) (err error) {
	fmt.Println("Getting JSON for mission IDs: ", missionIds)
	var result string
	txStart := time.Now()
	err = DB.Raw(`
WITH states AS (
  SELECT
    jsonb_build_array(json_build_array(ST_X(position),ST_Y(position),elevation_asl), bearing, lifestate, in_vehicle::int, unit_name, is_player::int, "current_role") AS state,
    soldier_id
  FROM
    soldier_states
    ORDER BY capture_frame ASC
),
soldiers AS (
  SELECT
    jsonb_build_object('joinTime', TO_CHAR(join_time, 'YYYY/MM/DD HH:MM:SS'), 'id', ocap_id, 'group', group_id, 'side', side, 'isPlayer', is_player, 'role', role_description, 'playerUid', player_uid, 'startFrameNum', join_frame,
      -- use custom value of "type"
      'type', 'unit', 'positions', json_agg(ss.state)) AS soldier
  FROM
    soldiers s
    LEFT JOIN states ss ON ss.soldier_id = s.id
  WHERE
    s.mission_id IN (?)
  GROUP BY
    s.id,
    s.join_time,
    s.ocap_id,
    s.group_id,
    s.side,
    s.is_player,
    s.role_description,
    s.player_uid,
    s.join_frame
)
SELECT
  jsonb_build_object(
    'entities', jsonb_agg(soldiers.soldier)
  )
FROM
  soldiers
LIMIT 500;
`, missionIds).Select("data").First(&result).Error

	fmt.Printf("Got JSON in %s\n", time.Since(txStart))

	if err != nil {
		return err
	}
	var jsondata map[string]interface{}

	// write raw result
	err = ioutil.WriteFile("demo_raw.json", []byte(result), 0644)
	if err != nil {
		panic(err)
	}

	// convert to JSON
	err = json.Unmarshal([]byte(result), &jsondata)
	if err != nil {
		panic(err)
	}

	// write compact json
	jsonBytes, err := json.Marshal(jsondata)
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile("demo.json", jsonBytes, 0644)
	if err != nil {
		panic(err)
	}

	fmt.Println("JSON written to demo.json")
	return nil
}

func main() {
	fmt.Println("Running DB connect/migrate to build schema...")
	err := getDB()
	if err != nil {
		panic(err)
	}
	fmt.Println("DB connect/migrate complete.")

	// get arguments
	args := os.Args[1:]
	if len(args) > 0 {
		if args[0] == "demo" {
			fmt.Println("Populating demo data...")
			TEST_DATA = true

			populateDemoData()
		}
		if args[0] == "getjson" {
			missionIds := args[1:]
			if len(missionIds) > 0 {
				fmt.Println("Getting JSON for mission IDs: ", missionIds)
				err = getDemoJSON(missionIds)
				if err != nil {
					panic(err)
				}
			} else {
				fmt.Println("No mission IDs provided.")
			}
		}
	} else {
		fmt.Println("No arguments provided.")
		fmt.Scanln()
	}
}
