package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	logging "github.com/sirupsen/logrus"
)

// Helper function to get the streamArn
func (ss *syncState) getStreamArn(key primaryKey) (string, error) {
	describeTableInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(ss.tableConfig.SrcTable),
	}
	// Remove after debugging
	logger.WithFields(logging.Fields{"TableName": key.sourceTable}).Debug()
	describeTableResult, err := ss.srcDynamo.DescribeTable(describeTableInput)
	if err != nil || describeTableResult.Table.StreamSpecification == nil {
		return "", errors.New(fmt.Sprintf(
			"Failed to get StreamARN for table %s. Check if stream is enabled",
			key.sourceTable),
		)
	}
	streamArn := describeTableResult.Table.LatestStreamArn
	logger.WithFields(logging.Fields{
		"StreamARN":    *streamArn,
		"Source Table": key.sourceTable,
	}).Info("Latest StreamARN")
	return *streamArn, nil
}

func (ss *syncState) shardSyncStart(key primaryKey,
	streamArn string,
	shard *dynamodbstreams.Shard) {
	var iterator *dynamodbstreams.GetShardIteratorOutput
	var records *dynamodbstreams.GetRecordsOutput
	var err error

	parentShardId := shard.ParentShardId
	shardId := shard.ShardId
	// process parent shard before child
	if parentShardId != nil {
		for !ss.isShardProcessed(key, parentShardId) {
			logger.WithFields(logging.Fields{
				"Shard Id":          *shardId,
				"Parent Shard Id":   *parentShardId,
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
			}).Debug("Waiting for parent shard to complete")
			time.Sleep(shardWaitForParentInterval)
		}
		logger.WithFields(logging.Fields{
			"Shard Id":          *shardId,
			"Parent Shard Id":   *parentShardId,
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Debug("Completed processing parent shard")
	}

	shardIteratorInput := ss.getShardIteratorInput(key, *shardId, streamArn)

	for i := 1; i <= maxRetries; i++ {
		iterator, err = ss.stream.GetShardIterator(shardIteratorInput)
		if err != nil {
			if i == maxRetries {
				logger.WithFields(logging.Fields{
					"Shard Id":          *shardId,
					"Error":             err,
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
				}).Error("GetShardIterator Error")
				return
			}
			backoff(i)
		} else {
			break
		}
	}

	shardIterator := iterator.ShardIterator

	// when nil, the shard has been closed, and the requested iterator
	// will not return any more data
	for shardIterator != nil {
		for i := 1; i <= maxRetries; i++ {

			records, err = ss.stream.GetRecords(&dynamodbstreams.GetRecordsInput{
				ShardIterator: shardIterator,
			})

			if err != nil {
				if i == maxRetries {
					logger.WithFields(logging.Fields{
						"Shard Id":          *shardId,
						"Error":             err,
						"Source Table":      key.sourceTable,
						"Destination Table": key.dstTable,
						"Iterator":          *shardIterator,
					}).Error("GetRecords Error")
					// Let this thread return,
					// but let a new thread process this shard
					ss.activeShardLock.Lock()
					delete(ss.activeShardProcessors, *shardId)
					ss.activeShardLock.Unlock()
					return
				}
				backoff(i)
			} else {
				break
			}
		}

		if len(records.Records) > 0 {
			logger.WithFields(logging.Fields{
				"Shard Id":          *shardId,
				"Records len":       len(records.Records),
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
			}).Debug("Shard sync, writing records")
			ss.writeRecords(records.Records, key, shard)
		}
		shardIterator = records.NextShardIterator
	}
	// Completed shard processing
	logger.WithFields(logging.Fields{
		"Shard Id":          *shardId,
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
	}).Debug("Shard Iterator returns nil")
	ss.markShardCompleted(key, shardId)
}

// Iterate through the records
// Depending on the action needed (MODIFY/INSERT/DELETE)
// perform required action on the dst table
// Once every `updateCheckpointThreshold` number of records are written,
// update the checkpoint
func (ss *syncState) writeRecords(
	records []*dynamodbstreams.Record,
	key primaryKey,
	shard *dynamodbstreams.Shard,
) {
	var err error
	for _, r := range records {
		err = nil
		switch *r.EventName {
		case "MODIFY":
			// same as insert
			fallthrough
		case "INSERT":
			err = ss.insertRecord(r.Dynamodb.NewImage, key)
		case "REMOVE":
			err = ss.removeRecord(r.Dynamodb.Keys, key)
		default:
			logger.WithFields(logging.Fields{
				"Event":             *r.EventName,
				"Record":            *r.Dynamodb,
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
				"Shard Id":          *shard.ShardId,
			}).Error("Unknown event on record")
		}

		if err != nil {
			logger.WithFields(logging.Fields{
				"Record":            *r.Dynamodb,
				"Event":             *r.EventName,
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
				"Shard Id":          *shard.ShardId,
				"Error":             err,
			}).Error("Failed to handle event")
		} else {
			logger.WithFields(logging.Fields{
				"Record":            *r.Dynamodb,
				"Event":             *r.EventName,
				"Source Table":      key.sourceTable,
				"Shard Id":          *shard.ShardId,
				"Destination Table": key.dstTable,
			}).Debug("Handled event successfully")
			ss.checkpointLock.Lock()
			ss.recordCounter++
			logger.WithFields(logging.Fields{
				"Counter":           ss.recordCounter,
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
				"Shard Id":          *shard.ShardId,
			}).Debug("Record counter")
			if ss.recordCounter == ss.tableConfig.UpdateCheckpointThreshold {
				ss.updateCheckpoint(key,
					*r.Dynamodb.SequenceNumber,
					shard)
				// reset the recordCounter
				ss.recordCounter = 0
			}
			ss.checkpointLock.Unlock()
		}
	}
}

// Insert this record in the dst table
func (ss *syncState) insertRecord(item map[string]*dynamodb.AttributeValue, key primaryKey) error {
	var err error

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(ss.tableConfig.DstTable),
	}
	for i := 1; i <= maxRetries; i++ {
		_, err = ss.dstDynamo.PutItem(input)
		if err == nil {
			return nil
		} else {
			backoff(i)
		}
	}
	return err
}

// Remove this record from the dst table
func (ss *syncState) removeRecord(item map[string]*dynamodb.AttributeValue, key primaryKey) error {
	var err error

	input := &dynamodb.DeleteItemInput{
		Key:       item,
		TableName: aws.String(ss.tableConfig.DstTable),
	}
	for i := 0; i < maxRetries; i++ {
		_, err = ss.dstDynamo.DeleteItem(input)
		if err == nil {
			return nil
		} else {
			backoff(i)
		}
	}
	return err
}
