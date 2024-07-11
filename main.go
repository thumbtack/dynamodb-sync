/*
 Copyright 2018-2024 Thumbtack, Inc.

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

	"github.com/aws/aws-sdk-go/service/dynamodb"
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

// The primary key of the Checkpoint ddb table, of the stream etc
// We need the key to be source + dest, since we can have a single
// source being synced with multiple destinations
type primaryKey struct {
	sourceTable string
	dstTable    string
}

type appConfig struct {
	sync []*syncConfig
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

	filepath := os.Getenv(paramConfigDir) + "/config.json"
	syncConfigs, err := parseConfigFile(filepath)
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
			"src_table": key.sourceTable,
			"dst_table": key.dstTable,
		}).Info("Launching replication...")

		syncWorker := NewSyncState(config, key)
		syncWorker.readCheckpoint()
		// Call a go routine to replicate for each key
		go syncWorker.replicate(quit)
	}

	http.HandleFunc("/", syncResponder())
	_ = http.ListenAndServe(":"+os.Getenv(paramPort), nil)
}

func syncResponder() http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(writer, "Hey there, I'm syncing")
	}
}
