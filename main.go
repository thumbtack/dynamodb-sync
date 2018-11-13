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
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/robfig/cron"
	logging "github.com/sirupsen/logrus"
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

var logger = logging.New()
var ddbTable = os.Getenv(paramCheckpointTable)
var ddbRegion = os.Getenv(paramCheckpointRegion)
var ddbEndpoint = os.Getenv(paramCheckpointEndpoint)
var maxRetries = defaultConfigMaxRetries
var ddbClient = ddbConfigConnect(ddbRegion, ddbEndpoint, maxRetries, *logger)

type config struct {
	SrcTable                  string `json:"src_table"`
	DstTable                  string `json:"dst_table"`
	SrcRegion                 string `json:"src_region"`
	DstRegion                 string `json:"dst_region"`
	SrcEndpoint               string `json:"src_endpoint"`
	DstEndpoint               string `json:"dst_endpoint"`
	SrcEnv                    string `json:"src_env"`
	DstEnv                    string `json:"dst_env"`
	MaxConnectRetries         int    `json:"max_connect_retries"`
	ReadWorkers               int    `json:"read_workers"`
	WriteWorkers              int    `json:"write_workers"`
	ReadQps                   int64  `json:"read_qps"`
	WriteQps                  int64  `json:"write_qps"`
	UpdateCheckpointThreshold int    `json:"update_checkpoint_threshold"`
	EnableStreaming           bool   `json:"enable_streaming"`
	TruncateTable             bool   `json:"truncate_table"`
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
	rateLimiterLock       sync.RWMutex
	recordCounter         int
	checkpoint            map[string]string
	expiredShards         map[string]bool
	timestamp             time.Time
}

func getRoleArn(env string) (string) {
	var roleType = ""
	if os.Getenv(paramConfigDir) != "shared" {
		roleType = "OLD_" + strings.ToUpper(env) + "_ROLE"
	} else {
		roleType = "NEW_" + strings.ToUpper(env) + "_ROLE"
	}
	logger.WithFields(logging.Fields{"Roletype": roleType}).Debug()
	return os.Getenv(roleType)
}

// syncState Constructor
func NewSyncState(tableConfig config) *syncState {
	var srcDynamo, dstDynamo *dynamodb.DynamoDB
	var stream *dynamodbstreams.DynamoDBStreams

	srcSess := session.Must(
		session.NewSession(
			aws.NewConfig().
				WithRegion(tableConfig.SrcRegion).
				WithEndpoint(tableConfig.SrcEndpoint).
				WithMaxRetries(tableConfig.MaxConnectRetries),
		))

	dstSess := session.Must(
		session.NewSession(
			aws.NewConfig().
				WithRegion(tableConfig.DstRegion).
				WithEndpoint(tableConfig.DstEndpoint).
				WithMaxRetries(tableConfig.MaxConnectRetries),
		))
	srcRoleArn := getRoleArn(tableConfig.SrcEnv)
	dstRoleArn := getRoleArn(tableConfig.DstEnv)

	if srcRoleArn == "" || dstRoleArn == "" {
		logger.WithFields(logging.Fields{}).
			Error("Unable to get RoleArn. " +
				"Check config file for env fields")
		return nil
	}
	logger.WithFields(logging.Fields{
		"Src Role Arn": srcRoleArn,
		"Dst Role Arn": dstRoleArn}).Debug("Role ARN")

	srcCreds := stscreds.NewCredentials(srcSess, srcRoleArn)
	dstCreds := stscreds.NewCredentials(dstSess, dstRoleArn)

	srcDynamo = dynamodb.New(srcSess, &aws.Config{Credentials: srcCreds})
	dstDynamo = dynamodb.New(dstSess, &aws.Config{Credentials: dstCreds})
	stream = dynamodbstreams.New(srcSess, &aws.Config{Credentials: srcCreds})

	return &syncState{
		tableConfig:           tableConfig,
		srcDynamo:             srcDynamo,
		dstDynamo:             dstDynamo,
		stream:                stream,
		completedShardLock:    sync.RWMutex{},
		activeShardProcessors: make(map[string]bool, 0),
		activeShardLock:       sync.RWMutex{},
		checkpointLock:        sync.RWMutex{},
		rateLimiterLock:       sync.RWMutex{},
		recordCounter:         0,
		checkpoint:            make(map[string]string, 0),
		expiredShards:         make(map[string]bool, 0),
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
	logger.SetLevel(logging.InfoLevel)
	var err error
	var configFile string
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
	if os.Getenv(paramMaxRetries) != "" {
		maxRetries, err = strconv.Atoi(os.Getenv(paramMaxRetries))
		if err != nil {
			logger.WithFields(logging.Fields{
				"error": err,
			}).Fatal("Failed to parse " + paramMaxRetries)
		}
	}

	configFile = os.Getenv(paramConfigDir) + "/config.json"
	tableConfig, err := readConfigFile(configFile, *logger)
	if err != nil {
		os.Exit(1)
	}

	tableConfig, err = setDefaults(tableConfig)
	if err != nil {
		logger.WithFields(logging.Fields{"Error": err}).Debug("Error in config file values")
	}

	return &appConfig{
		sync:    tableConfig,
		verbose: true,
	}
}

// Helper function to read the config file
func readConfigFile(configFile string, logger logging.Logger) ([]config, error) {
	var listStreamConfig []config
	logger.WithFields(logging.Fields{
		"path": configFile,
	}).Debug("Reading config file")
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return listStreamConfig, err
	}
	err = json.Unmarshal(data, &listStreamConfig)
	if err != nil {
		return listStreamConfig, errors.New("failed to unmarshal config")
	}

	return listStreamConfig, nil
}

// TODO: it would be better to start by assigning defaults and then overriding with the contents of the config file, no?
func setDefaults(tableConfig []config) ([]config, error) {
	var err error = nil
	for i := 0; i < len(tableConfig); i++ {
		if tableConfig[i].SrcTable == "" ||
			tableConfig[i].DstTable == "" ||
			tableConfig[i].SrcRegion == "" ||
			tableConfig[i].DstRegion == "" {
			err = errors.New("invalid JSON: source and destination table " +
				"and region are mandatory")
			continue
		}

		if tableConfig[i].MaxConnectRetries == 0 {
			tableConfig[i].MaxConnectRetries = 3
		}

		if tableConfig[i].ReadQps == 0 {
			tableConfig[i].ReadQps = 500
		}

		if tableConfig[i].WriteQps == 0 {
			tableConfig[i].WriteQps = 500
		}

		if tableConfig[i].ReadWorkers == 0 {
			tableConfig[i].ReadWorkers = 4
		}

		if tableConfig[i].WriteWorkers == 0 {
			tableConfig[i].WriteWorkers = 5
		}

		if tableConfig[i].UpdateCheckpointThreshold == 0 {
			tableConfig[i].UpdateCheckpointThreshold = 25
		}
	}

	return tableConfig, err
}

// If the state has no timestamp, or if the timestamp
// is more than 24 hours old, returns True. Else, False
func (sync *syncState) isFreshStart(key primaryKey) bool {
	logger.WithFields(logging.Fields{
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
		"State Timestamp":   sync.timestamp,
	}).Info("Checking if fresh start")
	if sync.timestamp.IsZero() ||
		time.Now().Sub(sync.timestamp) > streamRetentionHours {
		return true
	}
	return false
}

func main() {
	app := NewApp()
	quit := make(chan bool)
	for i := 0; i < len(app.sync); i++ {
		key := primaryKey{app.sync[i].SrcTable, app.sync[i].DstTable}
		logger.WithFields(logging.Fields{
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Info("Launching replicate")

		syncWorker := NewSyncState(app.sync[i])
		if syncWorker == nil {
			logger.WithFields(logging.Fields{
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
			}).Error("Error in connecting to tables. Check config file")

		}
		syncWorker.readCheckpoint()

		// Call a go routine to replicate for each key

		// Add a cron job if the schedule is once a week
		if !app.sync[i].EnableStreaming {
			c := cron.New()
			err := c.AddFunc("0 0 0 * * 5", func() {
				logger.WithFields(logging.Fields{
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
				}).Info("Starting cron job")
				syncWorker.replicate(quit, key)
			})
			if err != nil {
				logger.WithFields(logging.Fields{
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
					"Error":             err,
				}).Error("Error in replicating. Stopping cron")
				c.Stop()
			}
			c.Start()
		} else {
			// Streaming is enabled. Launch a go routine
			go syncWorker.replicate(quit, key)
		}
	}

	http.HandleFunc("/", syncResponder())
	http.ListenAndServe(":"+os.Getenv(paramPort), nil)
}
