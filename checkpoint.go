package main

import (
	"encoding/json"
	"time"

	logging "github.com/sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

const (
	sourceTable   = "source"
	dstTable      = "destination"
	timestp       = "Timestamp"
	checkpt       = "checkpoint"
	expiredShards = "ExpiredShards"
)

// Update checkpoint locally, and update checkpoint dynamodb table
func (app *appConfig) updateCheckpoint(key primaryKey,
	sequenceNumber string,
	shard *dynamodbstreams.Shard) {
	timestamp := time.Now().Format(time.RFC3339)
	if shard != nil {
		app.updateCheckpointLocal(key, sequenceNumber, shard, timestamp)
		app.updateCheckpointRemote(key, shard, timestamp)
	}
}

func (app *appConfig) updateCheckpointTimestamp(key primaryKey) {
	timestamp := time.Now().Format(time.RFC3339)
	app.updateTimestampLocal(key, timestamp)
	app.updateTimestampRemote(key, timestamp)
}

// Scan the checkpoint dynamodb table and parse it into the
// checkpoint Map of each primaryKey{src, dst}
func (app *appConfig) loadCheckpointTable() {
	app.logger.WithFields(logging.Fields{
		"Table": app.ddbTable,
	}).Info("Starting checkpoint table scan")
	lastEvaluatedKey := make(map[string]*dynamodb.AttributeValue, 0)
	maxConnectRetries := app.maxRetries
	items := make([]map[string]*dynamodb.AttributeValue, 0)
	input := &dynamodb.ScanInput{
		TableName: aws.String(app.ddbTable),
		Limit:     aws.Int64(int64(100)),
	}

	for {
		if len(lastEvaluatedKey) > 0 {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		for i := 0; i < maxConnectRetries; i++ {
			result, err := app.ddbClient.Scan(input)
			if err != nil {
				if i == maxConnectRetries-1 {
					app.logger.WithFields(logging.Fields{
						"Source Table":     app.ddbTable,
						"LastEvaluatedKey": input.ExclusiveStartKey,
						"Error":            err,
					}).Error("Failed to scan table in this iteration")
					return
				}
				app.backoff(i, "Scan")
			} else {
				lastEvaluatedKey = result.LastEvaluatedKey
				items = append(items, result.Items...)
				app.logger.WithFields(logging.Fields{
					"Scanned items size": len(result.Items),
					"Total items size":   len(items),
					"LastEvaluatedKey":   result.LastEvaluatedKey,
				}).Info("Checkpoint table scan successful")
				if len(lastEvaluatedKey) == 0 { // Done scanning
					app.processScannedCheckpoint(items)
					app.logger.WithFields(logging.Fields{}).
						Debug("Checkpoint table scan successfully completed")
					return
				}
				// Successfully connected to ddb. Don't retry connecting
				break
			}
		}
	}
}

/* Process scanned items
	 * items =>
	 * [{"Source":{"S":"src1"},
	 *	 "Destination":{"S":"dst1"},
	 *   "Checkpoint":{"B":{"ShardId":"sn1"}}
	 *   "ExpiredShards":{"B":{"ShardId":"true"}}
	 * 	},
	 * ]
*/
func (app *appConfig) processScannedCheckpoint(items []map[string]*dynamodb.AttributeValue) {
	app.logger.WithFields(logging.Fields{}).
		Info("Processing items scanned from checkpoint table")
	for i := range items {
		var err error
		key := primaryKey{*items[i][sourceTable].S, *items[i][dstTable].S}

		state := app.state[key]
		state.checkpoint = make(map[string]string, 0)
		state.expiredShards = make(map[string]bool, 0)
		if items[i][checkpt] != nil {
			err := json.Unmarshal(items[i][checkpt].B, &state.checkpoint)
			if err != nil {
				app.logger.WithFields(logging.Fields{
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
					"Error":             err,
				}).Error("Failed to unmarshal checkpoint")
			}
		}
		if items[i][expiredShards] != nil {
			err := json.Unmarshal(items[i][expiredShards].B, &state.expiredShards)
			if err != nil {
				app.logger.WithFields(logging.Fields{
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
					"Error":             err,
				}).Error("Failed to unmarshal expired shards")
			}
		}
		if items[i][timestp] != nil {
			state.timestamp, err = time.Parse(time.RFC3339, *items[i][timestp].S)
		}
		if err != nil {
			app.logger.WithFields(logging.Fields{
				"Checkpoint timestamp": *items[i][timestp].S,
				"Layout Pattern":       time.RFC3339,
				"Source Table":         key.sourceTable,
				"Destination Table":    key.dstTable,
			}).Error("Failed to parse checkpoint timestamp")
		}
		app.state[key] = state
	}

}

// Given a primaryKey, return whether it exists in the
// checkpoint dynamodb table
func (app *appConfig) isCheckpointFound(key primaryKey) (bool) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(app.ddbTable),
		Key: map[string]*dynamodb.AttributeValue{
			sourceTable: {S: aws.String(key.sourceTable)},
			dstTable:    {S: aws.String(key.dstTable)},
		},
	}
	lock := app.sync[key].checkpointLock
	lock.RLock()
	result, err := app.ddbClient.GetItem(input)
	lock.RUnlock()

	if err != nil {
		app.logger.WithFields(logging.Fields{
			"Error":             err,
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Error("Failed to read from checkpoint table")
		return false
	}

	if result.Item == nil {
		return false
	}

	app.logger.WithFields(logging.Fields{
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
	}).Debug("Found checkpoint record")

	return true
}

// Update the checkpoint for `key's` local state
// app.state[key]: timestamp,
// 				 `shardId`: `sequenceNumber`
func (app *appConfig) updateCheckpointLocal(
	key primaryKey,
	sequenceNumber string,
	shard *dynamodbstreams.Shard,
	timestamp string) {
	state, ok := app.state[key]
	if !ok {
		app.logger.WithFields(logging.Fields{
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Error("Failed to update checkpoint - unknown source table")
	}
	state.timestamp, _ = time.Parse(time.RFC3339, timestamp)
	state.checkpoint[*shard.ShardId] = sequenceNumber
	app.state[key] = state
}

// Update the checkpoint for `key` in the checkpoint dynamodb table
func (app *appConfig) updateCheckpointRemote(key primaryKey,
	shard *dynamodbstreams.Shard,
	timestamp string) {
	data, _ := json.Marshal(app.state[key].checkpoint)
	tableKey := map[string]*dynamodb.AttributeValue{
		sourceTable: {S: aws.String(key.sourceTable)},
		dstTable:    {S: aws.String(key.dstTable)},
	}

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(app.ddbTable),
		Key:       tableKey,
		ExpressionAttributeNames: map[string]*string{
			"#CP": aws.String(checkpt),
			"#TS": aws.String(timestp),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":val": {B: data},
			":ts":  {S: aws.String(timestamp)},
		},
		UpdateExpression: aws.String("SET #CP = :val, #TS = :ts"),
	}
	lock := app.sync[key].checkpointLock
	lock.Lock()
	_, err := app.ddbClient.UpdateItem(input)
	lock.Unlock()

	if err != nil {
		app.logger.WithFields(logging.Fields{
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
			"Sequence Number":   app.state[key].checkpoint[*shard.ShardId],
			"Timestamp":         timestamp,
			"Shard Id":          *shard.ShardId,
			"Error":             err,
		}).Error("Error in updating checkpoint on the global config")
	} else {
		app.logger.WithFields(logging.Fields{
			"Source Table":    key.sourceTable, "Destination Table": key.dstTable,
			"Sequence Number": app.state[key].checkpoint[*shard.ShardId],
			"Timestamp":       timestamp,
			"Shard Id":        *shard.ShardId,
		}).Debug("Successfully updated global config")
	}
}

func (app *appConfig) updateTimestampLocal(key primaryKey, timestamp string) {
	var err error
	state := app.state[key]
	state.timestamp, err = time.Parse(time.RFC3339, timestamp)
	if err != nil {
		app.logger.WithFields(logging.Fields{
			"Error":             err,
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Error("Failed to update local timestamp")
	}
	app.state[key] = state
}

func (app *appConfig) updateTimestampRemote(key primaryKey, timestamp string) {
	tableKey := map[string]*dynamodb.AttributeValue{
		sourceTable: {S: aws.String(key.sourceTable)},
		dstTable:    {S: aws.String(key.dstTable)},
	}
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(app.ddbTable),
		Key:       tableKey,
		ExpressionAttributeNames: map[string]*string{
			"#TS": aws.String(timestp),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":ts": {S: aws.String(timestamp)},
		},
		UpdateExpression: aws.String("SET #TS = :ts"),
	}

	_, err := app.ddbClient.UpdateItem(input)
	if err != nil {
		app.logger.WithFields(logging.Fields{
			"Error":             err,
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Error("Failed to update checkpoint timestamp")
	} else {
		app.logger.WithFields(logging.Fields{
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Debug("Successfully updated checkpoint timestamp")
	}
}

// We might need to drop stale checkpoints and expired shards
// while doing a fresh start
func (app *appConfig) dropCheckpoint(key primaryKey) {
	// Check if this item exists in checkpoint table
	if !app.isCheckpointFound(key) {
		return
	}
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			sourceTable: {S: aws.String(key.sourceTable)},
			dstTable:    {S: aws.String(key.dstTable)},
		},
		TableName: aws.String(app.ddbTable),
	}
	_, err := app.ddbClient.DeleteItem(input)
	if err != nil {
		app.logger.WithFields(logging.Fields{
			"Error":             err,
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Error("Error in dropping checkpoint")
	} else {
		app.logger.WithFields(logging.Fields{
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Debug("Dropped checkpoint successfully")
	}
}

// Remove checkpoint for <primaryKey, shardId> from the local state[key]
func (app *appConfig) expireCheckpointLocal(key primaryKey, shardId *string) {
	// Remove from activeShardProcessors
	lock := app.sync[key].activeShardLock
	lock.Lock()
	delete(app.sync[key].activeShardProcessors, *shardId)
	app.logger.WithFields(logging.Fields{
		"Shard Id":          *shardId,
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
	}).Debug("Deleted shard from active shards")
	lock.Unlock()

	// Add to expiredShards
	lock = app.sync[key].completedShardLock
	lock.Lock()
	state := app.state[key]
	state.expiredShards[*shardId] = true
	app.logger.WithFields(logging.Fields{
		"Shard Id":          *shardId,
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
	}).Debug("Added shard to expired shards")
	app.state[key] = state
	lock.Unlock()

	// Delete checkpoint for this shard
	lock = app.sync[key].checkpointLock
	lock.Lock()
	delete(app.state[key].checkpoint, *shardId)
	app.logger.WithFields(logging.Fields{
		"Shard Id":          *shardId,
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
	}).Debug("Marking shard as complete")
	lock.Unlock()
}

// Remove checkpoint for <primaryKey, shardId> from the checkpoint dynamodb table
func (app *appConfig) expireCheckpointRemote(key primaryKey, shardId string) {
	data, _ := json.Marshal(app.state[key].expiredShards)
	input := &dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			sourceTable: {S: aws.String(key.sourceTable)},
			dstTable:    {S: aws.String(key.dstTable)},
		},
		TableName: aws.String(app.ddbTable),
	}
	lock := app.sync[key].checkpointLock
	lock.Lock()
	input.SetExpressionAttributeNames(map[string]*string{
		"#CP":   aws.String(checkpt),
		"#SHID": aws.String(shardId),
	})
	input.SetUpdateExpression("REMOVE #CP.#SHID")
	_, err := app.ddbClient.UpdateItem(input)
	if err != nil {
		// That's fine, maybe this shard id did not have a checkpoint
		app.logger.WithFields(logging.Fields{
			"Error":             err,
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
			"Shard Id":          shardId,
		}).Error("Error in deleting checkpoint for shard id")
	} else {
		app.logger.WithFields(logging.Fields{
			"Source Table": key.sourceTable,
			"Destination Table": key.dstTable,
			"Shard Id": shardId,
		}).Debug("Successfully deleted checkpoint for shard id")
	}

	input.SetExpressionAttributeNames(map[string]*string{
		"#ExpSh": aws.String(expiredShards),
	})
	input.SetExpressionAttributeValues(map[string]*dynamodb.AttributeValue{
		":val": {B: data},
	})
	input.SetUpdateExpression("SET #ExpSh = :val")
	_, err = app.ddbClient.UpdateItem(input)
	if err != nil {
		app.logger.WithFields(logging.Fields{
			"Error":             err,
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
			"Shard Id":          shardId,
		}).Error("Error in adding expired shard")
	} else {
		app.logger.WithFields(logging.Fields{
			"Source Table": key.sourceTable,
			"Destination Table": key.dstTable,
			"Shard Id": shardId,
		}).Debug("Successfully added shard to expired shards")
	}

	lock.Unlock()
}
