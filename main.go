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
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	logging "github.com/sirupsen/logrus"
)

const (
	paramCheckpointTable    = "CHECKPOINT_DDB_TABLE"
	paramCheckpointRegion   = "CHECKPOINT_DDB_REGION"
	paramCheckpointEndpoint = "CHECKPOINT_DDB_ENDPOINT"
	paramVerbose            = "VERBOSE"
	paramPort               = "PORT"
	paramConfigDir          = "CONFIG_DIR"
	maxRetries              = 3
)

var logger = logging.New()
var ddbTable = os.Getenv(paramCheckpointTable)
var ddbRegion = os.Getenv(paramCheckpointRegion)
var ddbEndpoint = os.Getenv(paramCheckpointEndpoint)
var ddbClient = ddbConfigConnect(ddbRegion, ddbEndpoint, maxRetries, logger)

type config struct {
	SrcTable                  string `json:"src_table"`
	DstTable                  string `json:"dst_table"`
	SrcRegion                 string `json:"src_region"`
	DstRegion                 string `json:"dst_region"`
	SrcEndpoint               string `json:"src_endpoint"`
	DstEndpoint               string `json:"dst_endpoint"`
	SrcEnv                    string `json:"src_env"`
	DstEnv                    string `json:"dst_env"`
	ReadWorkers               int    `json:"read_workers"`
	WriteWorkers              int    `json:"write_workers"`
	ReadQPS                   int64  `json:"read_qps"`
	WriteQPS                  int64  `json:"write_qps"`
	UpdateCheckpointThreshold int    `json:"update_checkpoint_threshold"`
	EnableStreaming           *bool  `json:"enable_streaming"`
}

// Config file is read and dumped into this struct
type syncState struct {
	tableConfig           config
	srcDynamo             *dynamodb.DynamoDB
	dstDynamo             *dynamodb.DynamoDB
	stream                *dynamodbstreams.DynamoDBStreams
	completedShardLock    sync.RWMutex
	activeShardProcessors map[string]bool
	activeShardLock       sync.RWMutex
	checkpointLock        sync.RWMutex
	recordCounter         int
	checkpoint            map[string]string
	expiredShards         map[string]bool
	timestamp             time.Time
}

func getRoleArn(env string) string {
	roleType := strings.ToUpper(env) + "_ROLE"
	logger.Debugf("Roletype: %s", roleType)
	return os.Getenv(roleType)
}

// syncState Constructor
func NewSyncState(tableConfig config) *syncState {
	tr := &http.Transport{
		MaxIdleConns:    2048,
		MaxConnsPerHost: 1024,
	}
	httpClient := &http.Client{
		Timeout:   8 * time.Second,
		Transport: tr,
	}
	srcSess := session.Must(
		session.NewSession(
			aws.NewConfig().
				WithRegion(tableConfig.SrcRegion).
				WithEndpoint(tableConfig.SrcEndpoint).
				WithMaxRetries(maxRetries).
				WithHTTPClient(httpClient),
		))
	dstSess := session.Must(
		session.NewSession(
			aws.NewConfig().
				WithRegion(tableConfig.DstRegion).
				WithEndpoint(tableConfig.DstEndpoint).
				WithMaxRetries(maxRetries),
		))

	srcRoleArn := getRoleArn(tableConfig.SrcEnv)
	dstRoleArn := getRoleArn(tableConfig.DstEnv)
	if srcRoleArn == "" || dstRoleArn == "" {
		logger.Error("Unable to get RoleArn. Check config file for env fields")
		return nil
	}
	logger.WithFields(logging.Fields{
		"Src Role Arn": srcRoleArn,
		"Dst Role Arn": dstRoleArn,
	}).Debug("Role ARN")

	srcCreds := stscreds.NewCredentials(srcSess, srcRoleArn)
	dstCreds := stscreds.NewCredentials(dstSess, dstRoleArn)
	srcDynamo := dynamodb.New(srcSess, &aws.Config{Credentials: srcCreds})
	dstDynamo := dynamodb.New(dstSess, &aws.Config{Credentials: dstCreds})
	stream := dynamodbstreams.New(srcSess, &aws.Config{Credentials: srcCreds})

	return &syncState{
		tableConfig:           tableConfig,
		srcDynamo:             srcDynamo,
		dstDynamo:             dstDynamo,
		stream:                stream,
		completedShardLock:    sync.RWMutex{},
		activeShardProcessors: map[string]bool{},
		activeShardLock:       sync.RWMutex{},
		checkpointLock:        sync.RWMutex{},
		recordCounter:         0,
		checkpoint:            map[string]string{},
		expiredShards:         map[string]bool{},
		timestamp:             time.Time{},
	}
}

type appConfig struct {
	sync    []config
	verbose bool
}

// The primary key of the Checkpoint ddb table, of the stream etc
// We need the key to be source + dest, since we can have a single
// source being synced with multiple destinations
type primaryKey struct {
	sourceTable string
	dstTable    string
}

// Provisioned Read and Write Throughput of the ddb table
type provisionedThroughput struct {
	readCapacity  int64
	writeCapacity int64
}

// Creates dynamodb connection with the global checkpoint table
func ddbConfigConnect(
	region, endpoint string,
	maxRetries int,
	logger *logging.Logger,
) *dynamodb.DynamoDB {
	logger.Debug("Connecting to checkpoint table")
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
	logger.SetLevel(logging.InfoLevel)
	logger.SetFormatter(new(logging.JSONFormatter))

	if os.Getenv(paramVerbose) != "" {
		verbose, err := strconv.Atoi(os.Getenv(paramVerbose))
		if err != nil {
			logger.Fatalf("Failed to parse %s: %v", paramVerbose, err)
		}
		if verbose != 0 {
			logger.SetLevel(logging.DebugLevel)
		}
	}

	configFile := os.Getenv(paramConfigDir) + "/config.json"
	tableConfig, err := readConfigFile(configFile, logger)
	if err != nil {
		os.Exit(1)
	}

	tableConfig, err = setDefaults(tableConfig)
	if err != nil {
		logger.WithFields(logging.Fields{"Error": err}).Debug("Error in config file values")
		os.Exit(1)
	}

	return &appConfig{
		sync:    tableConfig,
		verbose: true,
	}
}

// Helper function to read the config file
func readConfigFile(
	configFile string,
	logger *logging.Logger,
) (listStreamConfig []config, err error) {
	logger.Debugf("Reading config file from %s", configFile)

	var data []byte
	data, err = ioutil.ReadFile(configFile)
	if err != nil {
		return
	}
	if err = json.Unmarshal(data, &listStreamConfig); err != nil {
		return listStreamConfig, fmt.Errorf("failed to unmarshal config: %v", err)
	}
	return
}

func setDefaults(tableConfig []config) ([]config, error) {
	for i, config := range tableConfig {
		if config.SrcTable == "" || config.DstTable == "" || config.SrcRegion == "" ||
			config.DstRegion == "" || config.SrcEnv == "" || config.DstEnv == "" {
			return nil, errors.New("invalid JSON: source and destination table " +
				"and region are mandatory")
		}
		if config.ReadQPS == 0 {
			tableConfig[i].ReadQPS = 500
		}
		if config.WriteQPS == 0 {
			tableConfig[i].WriteQPS = 500
		}
		if config.ReadWorkers == 0 {
			tableConfig[i].ReadWorkers = 4
		}
		if config.WriteWorkers == 0 {
			tableConfig[i].WriteWorkers = 5
		}
		if config.UpdateCheckpointThreshold == 0 {
			tableConfig[i].UpdateCheckpointThreshold = 25
		}
		if config.EnableStreaming == nil {
			val := true
			tableConfig[i].EnableStreaming = &val
		}
	}
	return tableConfig, nil
}

// If the state has no timestamp, or if the timestamp
// is more than 24 hours old, returns True. Else, False
func (ss *syncState) isFreshStart(key primaryKey) bool {
	logger.WithFields(logging.Fields{
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
		"State Timestamp":   ss.timestamp,
	}).Info("Checking if fresh start")
	return ss.timestamp.IsZero() || time.Now().Sub(ss.timestamp) > streamRetentionHours
}

func getPrimaryKey(sync config) primaryKey {
	key := primaryKey{
		sourceTable: sync.SrcTable,
		dstTable:    sync.DstTable,
	}
	if strings.Contains(sync.SrcEnv, "	new") {
		key.sourceTable += ".account." + strings.Split(sync.SrcEnv, "_")[0]
	}
	if strings.Contains(sync.DstEnv, "new") {
		key.dstTable += ".account." + strings.Split(sync.DstEnv, "_")[0]
	}
	return key
}

func main() {
	app := NewApp()
	quit := make(chan bool)
	for _, config := range app.sync {
		key := getPrimaryKey(config)
		logger.WithFields(logging.Fields{
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Info("Launching replicate")

		syncWorker := NewSyncState(config)
		if syncWorker == nil {
			logger.WithFields(logging.Fields{
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
			}).Error("Error in connecting to tables. Check config file")
			return
		}

		syncWorker.readCheckpoint()

		// Call a go routine to replicate for each key
		go syncWorker.replicate(quit, key)
	}

	http.HandleFunc("/", syncResponder())
	_ = http.ListenAndServe(":"+os.Getenv(paramPort), nil)
}

func syncResponder() http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(writer, "Hey there, I'm syncing")
	}
}
