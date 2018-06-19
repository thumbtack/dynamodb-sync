/*
 Copyright 2018 Thumbtack, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

/* Note: Although the tool allows multiple destinations to sync from a single source stream,
 * AWS DynamoDb Streams documentation specifies that, having more than 2 readers per shard
 * can result in throttling
 * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
*/

package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"net/http"
	"sync"
	"time"
	"strconv"
	_ "net/http/pprof"

	logging "github.com/sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/aws/session"
)

const (
	paramCheckpointTable    = "CHECKPOINT_DDB_TABLE"
	paramCheckpointRegion   = "CHECKPOINT_DDB_REGION"
	paramCheckpointEndpoint = "CHECKPOINT_DDB_ENDPOINT"
	paramMaxRetries         = "MAX_RETRIES"
	paramVerbose            = "VERBOSE"
	paramPort               = "PORT"
	paramConfigDir          = "CONFIG_DIR"
	defaultConfigMaxRetries = 3
)

// Config file is read and dumped into this struct
type syncConfig struct {
	SrcTable                  string `json:"src_table"`
	DstTable                  string `json:"dst_table"`
	SrcRegion                 string `json:"src_region"`
	DstRegion                 string `json:"dst_region"`
	SrcEndpoint               string `json:"src_endpoint"`
	DstEndpoint               string `json:"dst_endpoint"`
	MaxConnectRetries         int    `json:"max_connect_retries"`
	ReadWorkers               int    `json:"read_workers"`
	WriteWorkers              int    `json:"write_workers"`
	ReadQps                   int64  `json:"read_qps"`
	WriteQps                  int64  `json:"write_qps"`
	UpdateCheckpointThreshold int    `json:"update_checkpoint_threshold"`
	srcDynamo                 *dynamodb.DynamoDB
	dstDynamo                 *dynamodb.DynamoDB
	stream                    *dynamodbstreams.DynamoDBStreams
	completedShardLock        sync.RWMutex
	activeShardProcessors     map[string]bool
	activeShardLock           sync.RWMutex
	checkpointLock            sync.RWMutex
	recordCounter             int
}

// Monitor the progress of how much of the data has been copied
// from src to dst
// Especially helps in making scanning more efficient on
// larger tables
type copyProgress struct {
	totalReads     int64
	totalWrites    int64
	readCountLock  sync.RWMutex
	writeCountLock sync.RWMutex
}

// Checkpoint table on dynamodb is read and dumped into this struct
type stateTracker struct {
	// Multiple shards can be read in parallel
	// We need a checkpoint for each of those shards
	// Therefore it is a map of <shardId, checkpoint>
	checkpoint    map[string]string
	expiredShards map[string]bool
	timestamp     time.Time
}

type appConfig struct {
	state      map[primaryKey]stateTracker
	sync       map[primaryKey]syncConfig
	progress   map[primaryKey]copyProgress
	ddbTable   string
	ddbRegion  string
	ddbClient  *dynamodb.DynamoDB
	maxRetries int
	verbose    bool
	logger     logging.Logger
}

// The primary key of the Checkpoint ddb table, of the stream etc
// We need the key to be source + dest, since we can have a single
// source being synced with multiple destinations
type primaryKey struct {
	sourceTable string
	dstTable    string
}

// Creates dynamodb connection with the global checkpoint table
func ddbConfigConnect(region string, endpoint string, maxRetries int, logger logging.Logger) *dynamodb.DynamoDB {
	logger.WithFields(logging.Fields{}).Debug("Connecting to checkpoint table")
	return dynamodb.New(session.Must(
		session.NewSession(
			aws.NewConfig().
				WithRegion(region).
				WithEndpoint(endpoint).
				WithMaxRetries(maxRetries),
		)))
}

// app constructor
func NewApp() *appConfig {
	// set up logging
	logger := logging.New()
	logger.SetLevel(logging.InfoLevel)
	ddbRegion := os.Getenv(paramCheckpointRegion)
	ddbEndpoint := os.Getenv(paramCheckpointEndpoint)

	var err error

	if os.Getenv(paramVerbose) != "" {
		verbose, err := strconv.Atoi(os.Getenv(paramVerbose))
		if err != nil {
			logger.WithFields(logging.Fields{
				"error": err,
			}).Fatal("Failed to parse " + paramVerbose)
		}
		if verbose != 0 {
			logger.SetLevel(logging.DebugLevel)
		}
	}
	maxRetries := defaultConfigMaxRetries
	if os.Getenv(paramMaxRetries) != "" {
		maxRetries, err = strconv.Atoi(os.Getenv(paramMaxRetries))
		if err != nil {
			logger.WithFields(logging.Fields{
				"error": err,
			}).Fatal("Failed to parse " + paramMaxRetries)
		}
	}
	configFile := os.Getenv(paramConfigDir) + "/config.json"
	streamMap, err := readConfigFile(configFile, *logger)
	if err != nil {
		logger.WithFields(logging.Fields{"error": err}).Error("Failed to read config file")
		os.Exit(1)
	}

	progress := make(map[primaryKey]copyProgress, 0)
	for k, _ := range streamMap {
		progress[k] = copyProgress{totalReads: 0, totalWrites: 0}
	}

	return &appConfig{
		state:      make(map[primaryKey]stateTracker, 0),
		sync:       streamMap,
		progress:   progress,
		ddbTable:   os.Getenv(paramCheckpointTable),
		ddbRegion:  ddbRegion,
		ddbClient:  ddbConfigConnect(ddbRegion, ddbEndpoint, maxRetries, *logger),
		maxRetries: maxRetries,
		verbose:    true,
		logger:     *logger,
	}
}

// state tracker constructor
func NewStateTracker() *stateTracker {
	return &stateTracker{
		checkpoint:    make(map[string]string, 0),
		expiredShards: make(map[string]bool, 0),
		timestamp:     time.Time{},
	}
}

// Helper function to read the config file
func readConfigFile(configFile string, logger logging.Logger) (map[primaryKey]syncConfig, error) {
	result := make(map[primaryKey]syncConfig, 0)
	logger.WithFields(logging.Fields{
		"path": configFile,
	}).Debug("Reading config file")
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return result, err
	}

	return parseConfig(data)
}

// Parses the contents of the stream config file
// and creates a map with
// key: primaryKey{src, dst},
// value: syncConfig
func parseConfig(jsonData []byte) (map[primaryKey]syncConfig, error) {
	var listStreamConfig []syncConfig
	mapStreamConfig := make(map[primaryKey]syncConfig)

	err := json.Unmarshal(jsonData, &listStreamConfig)
	if err != nil {
		return nil, err
	}
	for i := range listStreamConfig {
		key := primaryKey{
			listStreamConfig[i].SrcTable,
			listStreamConfig[i].DstTable,
		}
		listStreamConfig[i].activeShardProcessors = make(map[string]bool, 0)
		mapStreamConfig[key] = listStreamConfig[i]
	}

	mapStreamConfig, err = setDefaults(mapStreamConfig)
	return mapStreamConfig, err
}

// TODO: it would be better to start by assigning defaults and then overriding with the contents of the config file, no?
func setDefaults(mapStreamConfig map[primaryKey]syncConfig) (
	map[primaryKey]syncConfig, error,
) {
	for key, streamConf := range mapStreamConfig {
		if streamConf.SrcTable == "" ||
			streamConf.DstTable == "" ||
			streamConf.SrcRegion == "" ||
			streamConf.DstRegion == "" {
			err := errors.New("invalid JSON: source and destination table and region are mandatory")
			return nil, err
		}

		if streamConf.MaxConnectRetries == 0 {
			streamConf.MaxConnectRetries = 3
		}

		if streamConf.ReadQps == 0 {
			streamConf.ReadQps = 500
		}

		if streamConf.WriteQps == 0 {
			streamConf.WriteQps = 500
		}

		if streamConf.ReadWorkers == 0 {
			streamConf.ReadWorkers = 4
		}

		if streamConf.WriteWorkers == 0 {
			streamConf.WriteWorkers = 5
		}

		if streamConf.UpdateCheckpointThreshold == 0 {
			streamConf.UpdateCheckpointThreshold = 25
		}

		mapStreamConfig[key] = streamConf
	}

	return mapStreamConfig, nil
}

// If the state has no timestamp, or if the timestamp
// is more than 24 hours old, returns True. Else, False
func (app *appConfig) isFreshStart(key primaryKey) (bool) {
	state := app.state[key]
	app.logger.WithFields(logging.Fields{
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
		"State Timestamp":   state.timestamp,
	}).Info("Checking if fresh start")
	if state.timestamp.IsZero() ||
		time.Now().Sub(state.timestamp) > streamRetentionHours {
		return true
	}
	return false
}

// Creates dynamoDB connections with the source, dst, and the source stream
func (app *appConfig) connect() {
	for key := range app.sync {
		streamConf := app.sync[key]
		streamConf.srcDynamo = dynamodb.New(session.Must(
			session.NewSession(
				aws.NewConfig().
					WithRegion(streamConf.SrcRegion).
					WithEndpoint(streamConf.SrcEndpoint).
					WithMaxRetries(streamConf.MaxConnectRetries),
			)))
		streamConf.dstDynamo = dynamodb.New(session.Must(
			session.NewSession(
				aws.NewConfig().
					WithRegion(streamConf.DstRegion).
					WithEndpoint(streamConf.DstEndpoint).
					WithMaxRetries(streamConf.MaxConnectRetries),
			)))
		streamConf.stream = dynamodbstreams.New(session.Must(
			session.NewSession(
				aws.NewConfig().
					WithRegion(streamConf.SrcRegion).
					WithEndpoint(streamConf.SrcEndpoint).
					WithMaxRetries(streamConf.MaxConnectRetries),
			)))
		app.sync[key] = streamConf
	}
}

// Looks for <src,dst> in the config file, not present in the
// global checkpoint table
// These are the newly added primaryKeys. This function creates
// a stateTracker, and adds them to the state
func (app *appConfig) addNewStateTracker() {
	for key := range app.sync {
		_, ok := app.state[key]
		if !ok {
			app.logger.WithFields(logging.Fields{
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
			}).Info("Adding new states")
			app.state[key] = *NewStateTracker()
		}
	}
}

func (app *appConfig) removeOldStateTracker() {
	for key, _ := range app.state {
		_, ok := app.sync[key]
		if !ok {
			// Present in `state` but not in `sync`
			app.logger.WithFields(logging.Fields{
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
			}).Info("Removing old keys")
			//app.dropCheckpoint(key)
			delete(app.state, key)
		}
	}
}

func main() {
	app := NewApp()

	// establish connections to DynamoDB (for all (src, dst) pairs)
	app.connect()

	// Scan the global checkpoint table
	// Parse its contents into state map with
	// key: primaryKey{src, dst}
	// value: stateTracker
	app.logger.WithFields(
		logging.Fields{},
	).Info("Launching checkpoint table scan")
	app.loadCheckpointTable()

	// If there are any new primaryKey{src, dst} in the
	// local stream config file, add them to the state
	app.addNewStateTracker()

	// If there are any old primaryKey{src, dst} in the
	// remote checkpoint table which have been removed
	// from the streamConfig, remove them from the stateMap
	// and checkpoint table
	app.removeOldStateTracker()
	quit := make(chan bool)

	for key, _ := range app.sync {
		app.logger.WithFields(logging.Fields{
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Debug("Launching replicate")
		// Call a go routine to replicate for each key
		go app.replicate(quit, key)
	}

	http.HandleFunc("/", syncResponder())
	http.ListenAndServe(":"+os.Getenv(paramPort), nil)
}
