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
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	logging "github.com/sirupsen/logrus"
)

const (
	paramVerbose   = "VERBOSE"
	paramPort      = "PORT"
	paramConfigDir = "CONFIG_DIR"
	maxRetries     = 3

	paramCheckpointTable    = "CHECKPOINT_DDB_TABLE"
	paramCheckpointRegion   = "CHECKPOINT_DDB_REGION"
	paramCheckpointEndpoint = "CHECKPOINT_DDB_ENDPOINT"
)

var (
	logger      = logging.New()
	ddbTable    = os.Getenv(paramCheckpointTable)
	ddbRegion   = os.Getenv(paramCheckpointRegion)
	ddbEndpoint = os.Getenv(paramCheckpointEndpoint)
	ddbClient   = dynamodb.New(getSession(ddbRegion, ddbEndpoint, nil))
)

// Config file is read and dumped into this struct
type syncState struct {
	tableConfig           syncConfig
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

// syncState Constructor
func NewSyncState(tableConfig syncConfig) *syncState {
	httpClient := &http.Client{
		Timeout: 8 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:    2048,
			MaxConnsPerHost: 1024,
		},
	}
	srcSess := getSession(tableConfig.SrcRegion, tableConfig.SrcEndpoint, httpClient)
	dstSess := getSession(tableConfig.DstRegion, tableConfig.DstEndpoint, nil)
	srcRoleArn := getRoleArn(tableConfig.SrcEnv)
	dstRoleArn := getRoleArn(tableConfig.DstEnv)
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
	sync []*syncConfig
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

// NewApp sets up the app configuration
func NewApp() (*appConfig, error) {
	logger.SetLevel(logging.InfoLevel)
	logger.SetFormatter(new(logging.JSONFormatter))

	if verboseStr := os.Getenv(paramVerbose); verboseStr != "" {
		verbose, err := strconv.Atoi(verboseStr)
		if err != nil {
			logger.Warnf("failed to parse %s: %v", paramVerbose, err)
		}
		if verbose != 0 {
			logger.SetLevel(logging.DebugLevel)
		}
	}

	configFile := os.Getenv(paramConfigDir) + "/config.json"
	syncConfigs, err := parseConfigFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config file: %v", err)
	}
	for _, config := range syncConfigs {
		if err := config.setDefault(); err != nil {
			return nil, fmt.Errorf("failed to set default: %v", err)
		}
	}

	return &appConfig{
		sync: syncConfigs,
	}, nil
}

func main() {
	app, err := NewApp()
	if err != nil {
		logger.Errorf("failed to initialize the app: %v", err)
		os.Exit(1)
	}
	quit := make(chan bool)
	for _, config := range app.sync {
		key := config.getCheckpointPK()
		logger.WithFields(logging.Fields{
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Info("Launching replicate")

		syncWorker := NewSyncState(*config)
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
