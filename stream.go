package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	logging "github.com/sirupsen/logrus"
)

// getStreamArn gets the streamArn
func (ss *syncState) getStreamArn() (string, error) {
	describeTableResult, err := ss.srcDynamo.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(ss.tableConfig.SrcTable),
	})
	if err != nil || describeTableResult.Table.StreamSpecification == nil {
		return "", fmt.Errorf("failed to get StreamARN for table %s. Check if stream is enabled",
			ss.checkpointPK.sourceTable)
	}
	streamArn := describeTableResult.Table.LatestStreamArn
	logger.WithFields(logging.Fields{
		"stream arn": *streamArn,
		"src table":  ss.checkpointPK.sourceTable,
	}).Info("Latest StreamARN")
	return *streamArn, nil
}

func (ss *syncState) shardSyncStart(streamArn string, shard *dynamodbstreams.Shard) {
	logField := logging.Fields{
		"shardID":        *shard.ShardId,
		"parent shardID": *shard.ParentShardId,
		"src table":      ss.checkpointPK.sourceTable,
		"dst table":      ss.checkpointPK.dstTable,
	}
	// process parent shard before child
	if shard.ParentShardId != nil {
		for !ss.isShardProcessed(shard.ParentShardId) {
			logger.WithFields(logField).Debug("Waiting for parent shard to complete")
			time.Sleep(shardWaitForParentInterval)
		}
		logger.WithFields(logField).Debug("Completed processing parent shard")
	}

	var shardIterator *string
	shardIteratorInput := ss.getShardIteratorInput(*shard.ShardId, streamArn)
	for i := 1; i <= maxRetries; i++ {
		shardIteratorOutput, err := ss.stream.GetShardIterator(shardIteratorInput)
		if err == nil {
			shardIterator = shardIteratorOutput.ShardIterator
			break
		}
		if i == maxRetries {
			logField["error"] = err
			logger.WithFields(logField).Error("failed in GetShardIterator")
			return
		}
		backoff(i)
	}

	// the shard is closed when nil, the requested iterator will not return any more data
	for shardIterator != nil {
		var records []*dynamodbstreams.Record
		for i := 1; i <= maxRetries; i++ {
			getRecordsOutput, err := ss.stream.GetRecords(&dynamodbstreams.GetRecordsInput{
				ShardIterator: shardIterator,
			})
			if err == nil {
				records = getRecordsOutput.Records
				shardIterator = getRecordsOutput.NextShardIterator
				break
			}
			if i == maxRetries {
				logField["iterator"] = *shardIterator
				logField["error"] = err
				logger.WithFields(logField).Error("GetRecords Error")

				// let this thread return, and let a new one processes this shard
				ss.activeShardLock.Lock()
				delete(ss.activeShardProcessors, *shard.ShardId)
				ss.activeShardLock.Unlock()
				return
			}
			backoff(i)
		}

		if len(records) > 0 {
			logField["record length"] = len(records)
			logger.WithFields(logField).Debug("shard synced, start writing records")
			ss.writeRecords(records, shard)
		}
	}
	// Completed shard processing
	logger.WithFields(logField).Debug("shard Iterator returns nil")
	ss.markShardCompleted(shard.ShardId)
}

// writeRecords iterates through the records. Depending on the action needed (MODIFY/INSERT/DELETE),
// perform required action on the dst table.
// Once every `updateCheckpointThreshold` number of records are written, update the checkpoint.
func (ss *syncState) writeRecords(records []*dynamodbstreams.Record, shard *dynamodbstreams.Shard) {
	var err error
	for _, r := range records {
		logField := logging.Fields{
			"record":    *r.Dynamodb,
			"event":     *r.EventName,
			"src table": ss.checkpointPK.sourceTable,
			"dst table": ss.checkpointPK.dstTable,
			"shard ID":  *shard.ShardId,
		}

		err = nil
		switch *r.EventName {
		case "MODIFY", "INSERT":
			err = ss.insertRecord(r.Dynamodb.NewImage)
		case "REMOVE":
			err = ss.removeRecord(r.Dynamodb.Keys)
		default:
			logger.WithFields(logField).Error("unknown event on the record")
		}

		if err != nil {
			logField["error"] = err
			logger.WithFields(logField).Error("failed to handle the event")
		} else {
			logger.WithFields(logField).Debug("successfully handled the event")
			ss.checkpointLock.Lock()
			ss.recordCounter++
			logField["counter"] = ss.recordCounter
			logger.WithFields(logField).Debug("record counter")
			if ss.recordCounter == ss.tableConfig.UpdateCheckpointThreshold {
				ss.updateCheckpoint(*r.Dynamodb.SequenceNumber, shard)
				// reset the recordCounter
				ss.recordCounter = 0
			}
			ss.checkpointLock.Unlock()
		}
	}
}

// insertRecord inserts the record into the dst table
func (ss *syncState) insertRecord(item map[string]*dynamodb.AttributeValue) (err error) {
	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(ss.tableConfig.DstTable),
	}
	for i := 1; i <= maxRetries; i++ {
		if _, err = ss.dstDynamo.PutItem(input); err == nil || i == maxRetries {
			break
		}
		backoff(i)
	}
	return
}

// removeRecord removes the record from the dst table
func (ss *syncState) removeRecord(item map[string]*dynamodb.AttributeValue) (err error) {
	input := &dynamodb.DeleteItemInput{
		Key:       item,
		TableName: aws.String(ss.tableConfig.DstTable),
	}
	for i := 1; i <= maxRetries; i++ {
		if _, err = ss.dstDynamo.DeleteItem(input); err == nil || i == maxRetries {
			break
		}
		backoff(i)
	}
	return
}
