package main

import (
	"encoding/json"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	logging "github.com/sirupsen/logrus"
)

const (
	sourceTable   = "source"
	dstTable      = "destination"
	timestp       = "Timestamp"
	checkpt       = "checkpoint"
	expiredShards = "ExpiredShards"
)

// Update checkpoint locally, and update checkpoint dynamodb table
func (ss *syncState) updateCheckpoint(sequenceNumber string, shard *dynamodbstreams.Shard) {
	timestamp := time.Now()
	if shard != nil {
		ss.updateCheckpointLocal(sequenceNumber, shard, timestamp)
		ss.updateCheckpointRemote(shard.ShardId, timestamp)
	}
}

func (ss *syncState) updateCheckpointTimestamp() {
	timestamp := time.Now()
	ss.updateTimestampLocal(timestamp)
	ss.updateTimestampRemote(timestamp)
}

func (ss *syncState) readCheckpoint() {
	logger.WithFields(logging.Fields{
		"Table":             ddbTable,
		"Source Table":      ss.tableConfig.SrcTable,
		"Destination Table": ss.tableConfig.DstTable,
	}).Info("Reading checkpoint table")

	input := &dynamodb.GetItemInput{
		TableName: aws.String(ddbTable),
		Key: map[string]*dynamodb.AttributeValue{
			sourceTable: {S: aws.String(ss.tableConfig.SrcTable)},
			dstTable:    {S: aws.String(ss.tableConfig.DstTable)},
		},
	}
	result, err := ddbClient.GetItem(input)
	if err != nil {
		logger.WithFields(logging.Fields{
			"Error":             err,
			"Source Table":      ss.tableConfig.SrcTable,
			"Destination Table": ss.tableConfig.DstTable,
		}).Error("Failed to read from checkpoint table")
	}
	if result == nil || result.Item == nil {
		return
	}
	if result.Item[checkpt] != nil {
		err = json.Unmarshal(result.Item[checkpt].B, &ss.checkpoint)
		if err != nil {
			logger.WithFields(logging.Fields{
				"Source Table":      ss.tableConfig.SrcTable,
				"Destination Table": ss.tableConfig.DstTable,
				"Error":             err,
			}).Error("Failed to unmarshal checkpoint")
		}
	}
	if result.Item[expiredShards] != nil {
		err = json.Unmarshal(result.Item[expiredShards].B, &ss.expiredShards)
		if err != nil {
			logger.WithFields(logging.Fields{
				"Source Table":      ss.tableConfig.SrcTable,
				"Destination Table": ss.tableConfig.DstTable,
				"Error":             err,
			}).Error("Failed to unmarshal expired shards")
		}
	}
	ss.timestamp, err = time.Parse(time.RFC3339, *result.Item[timestp].S)
	if err != nil {
		logger.WithFields(logging.Fields{
			"Checkpoint timestamp": *result.Item[timestp].S,
			"Layout Pattern":       time.RFC3339,
			"Source Table":         ss.tableConfig.SrcTable,
			"Destination Table":    ss.tableConfig.DstTable,
			"Error":                err,
		}).Error("Failed to parse checkpoint timestamp")
	}
}

// Given a primaryKey, return whether it exists in the
// checkpoint dynamodb table
func (ss *syncState) isCheckpointFound() bool {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(ddbTable),
		Key: map[string]*dynamodb.AttributeValue{
			sourceTable: {S: aws.String(ss.checkpointPK.sourceTable)},
			dstTable:    {S: aws.String(ss.checkpointPK.dstTable)},
		},
	}
	result, err := ddbClient.GetItem(input)

	if err != nil {
		logger.WithFields(logging.Fields{
			"Error":             err,
			"Source Table":      ss.checkpointPK.sourceTable,
			"Destination Table": ss.checkpointPK.dstTable,
		}).Error("Failed to read from checkpoint table")
		return false
	}

	if result.Item == nil {
		return false
	}

	logger.WithFields(logging.Fields{
		"Source Table":      ss.checkpointPK.sourceTable,
		"Destination Table": ss.checkpointPK.dstTable,
	}).Debug("Found checkpoint record")

	return true
}

// updateCheckpointLocal updates the checkpoint for `key's` local state sync
func (ss *syncState) updateCheckpointLocal(
	sequenceNumber string,
	shard *dynamodbstreams.Shard,
	timestamp time.Time,
) {
	ss.timestamp = timestamp
	ss.checkpoint[*shard.ShardId] = sequenceNumber
}

// updateCheckpointRemote updates the checkpoint for `key` in the checkpoint dynamodb table
func (ss *syncState) updateCheckpointRemote(shardId *string, timestamp time.Time) {
	data, _ := json.Marshal(ss.checkpoint)
	tableKey := map[string]*dynamodb.AttributeValue{
		sourceTable: {S: aws.String(ss.checkpointPK.sourceTable)},
		dstTable:    {S: aws.String(ss.checkpointPK.dstTable)},
	}

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(ddbTable),
		Key:       tableKey,
		ExpressionAttributeNames: map[string]*string{
			"#CP": aws.String(checkpt),
			"#TS": aws.String(timestp),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":val": {B: data},
			":ts":  {S: aws.String(timestamp.Format(time.RFC3339))},
		},
		UpdateExpression: aws.String("SET #CP = :val, #TS = :ts"),
	}

	_, err := ddbClient.UpdateItem(input)

	if err != nil {
		logger.WithFields(logging.Fields{
			"Source Table":      ss.checkpointPK.sourceTable,
			"Destination Table": ss.checkpointPK.dstTable,
			"Sequence Number":   ss.checkpoint[*shardId],
			"Timestamp":         timestamp,
			"Shard Id":          *shardId,
			"Error":             err,
		}).Error("Error in updating checkpoint on the global config")
	} else {
		logger.WithFields(logging.Fields{
			"Source Table":      ss.checkpointPK.sourceTable,
			"Destination Table": ss.checkpointPK.dstTable,
			"Sequence Number":   ss.checkpoint[*shardId],
			"Timestamp":         timestamp,
			"Shard Id":          *shardId,
		}).Debug("Successfully updated global config")
	}
}

func (ss *syncState) updateTimestampLocal(timestamp time.Time) {
	ss.timestamp = timestamp
}

func (ss *syncState) updateTimestampRemote(timestamp time.Time) {
	tableKey := map[string]*dynamodb.AttributeValue{
		sourceTable: {S: aws.String(ss.checkpointPK.sourceTable)},
		dstTable:    {S: aws.String(ss.checkpointPK.dstTable)},
	}
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(ddbTable),
		Key:       tableKey,
		ExpressionAttributeNames: map[string]*string{
			"#TS": aws.String(timestp),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":ts": {S: aws.String(timestamp.Format(time.RFC3339))},
		},
		UpdateExpression: aws.String("SET #TS = :ts"),
	}

	_, err := ddbClient.UpdateItem(input)
	if err != nil {
		logger.WithFields(logging.Fields{
			"Error":             err,
			"Source Table":      ss.checkpointPK.sourceTable,
			"Destination Table": ss.checkpointPK.dstTable,
		}).Error("Failed to update checkpoint timestamp")
	} else {
		logger.WithFields(logging.Fields{
			"Source Table":      ss.checkpointPK.sourceTable,
			"Destination Table": ss.checkpointPK.dstTable,
		}).Debug("Successfully updated checkpoint timestamp")
	}
}

// We might need to drop stale checkpoints and expired shards
// while doing a fresh start
func (ss *syncState) dropCheckpoint() {
	// Check if this item exists in checkpoint table
	if !ss.isCheckpointFound() {
		return
	}
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			sourceTable: {S: aws.String(ss.checkpointPK.sourceTable)},
			dstTable:    {S: aws.String(ss.checkpointPK.dstTable)},
		},
		TableName: aws.String(ddbTable),
	}
	_, err := ddbClient.DeleteItem(input)
	if err != nil {
		logger.WithFields(logging.Fields{
			"Error":             err,
			"Source Table":      ss.checkpointPK.sourceTable,
			"Destination Table": ss.checkpointPK.dstTable,
		}).Error("Error in dropping checkpoint")
	} else {
		logger.WithFields(logging.Fields{
			"Source Table":      ss.checkpointPK.sourceTable,
			"Destination Table": ss.checkpointPK.dstTable,
		}).Debug("Dropped checkpoint successfully")
	}
}

// Remove checkpoint for <primaryKey, shardId> from the local state[key]
func (ss *syncState) expireCheckpointLocal(shardId *string) {
	// Remove from activeShardProcessors
	ss.activeShardLock.Lock()
	delete(ss.activeShardProcessors, *shardId)
	logger.WithFields(logging.Fields{
		"Shard Id":          *shardId,
		"Source Table":      ss.checkpointPK.sourceTable,
		"Destination Table": ss.checkpointPK.dstTable,
	}).Debug("Deleted shard from active shards")
	ss.activeShardLock.Unlock()

	// Add to expiredShards
	ss.completedShardLock.Lock()
	ss.expiredShards[*shardId] = true
	logger.WithFields(logging.Fields{
		"Shard Id":          *shardId,
		"Source Table":      ss.checkpointPK.sourceTable,
		"Destination Table": ss.checkpointPK.dstTable,
	}).Debug("Added shard to expired shards")
	ss.completedShardLock.Unlock()

	// Delete checkpoint for this shard
	ss.checkpointLock.Lock()
	delete(ss.checkpoint, *shardId)
	logger.WithFields(logging.Fields{
		"Shard Id":          *shardId,
		"Source Table":      ss.checkpointPK.sourceTable,
		"Destination Table": ss.checkpointPK.dstTable,
	}).Debug("Marking shard as complete")
	ss.checkpointLock.Unlock()
}

// Remove checkpoint for <primaryKey, shardId> from the checkpoint dynamodb table
func (ss *syncState) expireCheckpointRemote(shardId string) {
	data, _ := json.Marshal(ss.expiredShards)
	ss.updateCheckpointRemote(&shardId, ss.timestamp)

	ss.checkpointLock.Lock()
	input := &dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			sourceTable: {S: aws.String(ss.checkpointPK.sourceTable)},
			dstTable:    {S: aws.String(ss.checkpointPK.dstTable)},
		},
		TableName: aws.String(ddbTable),
	}
	input.SetExpressionAttributeNames(map[string]*string{
		"#ExpSh": aws.String(expiredShards),
	})
	input.SetExpressionAttributeValues(map[string]*dynamodb.AttributeValue{
		":val": {B: data},
	})
	input.SetUpdateExpression("SET #ExpSh = :val")
	_, err := ddbClient.UpdateItem(input)
	if err != nil {
		logger.WithFields(logging.Fields{
			"Error":             err,
			"Source Table":      ss.checkpointPK.sourceTable,
			"Destination Table": ss.checkpointPK.dstTable,
			"Shard Id":          shardId,
		}).Error("Error in adding expired shard")
	} else {
		logger.WithFields(logging.Fields{
			"Source Table":      ss.checkpointPK.sourceTable,
			"Destination Table": ss.checkpointPK.dstTable,
			"Shard Id":          shardId,
		}).Debug("Successfully added shard to expired shards")
	}

	ss.checkpointLock.Unlock()
}
