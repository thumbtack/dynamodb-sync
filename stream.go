package main

import (
	"errors"
	"fmt"
	"time"

	logging "github.com/sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

// Helper function to get the streamArn
func (app *appConfig) getStreamArn(key primaryKey) (string, error) {
	describeTableInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(key.sourceTable),
	}
	describeTableResult, err := app.sync[key].srcDynamo.DescribeTable(describeTableInput)
	if describeTableResult.Table.StreamSpecification == nil ||
		err != nil {
		return "", errors.New(fmt.Sprintf(
			"Failed to get StreamARN for table %s. Check if stream is enabled",
			key.sourceTable),
		)
	}
	streamArn := describeTableResult.Table.LatestStreamArn
	app.logger.WithFields(logging.Fields{
		"StreamARN":    streamArn,
		"Source Table": key.sourceTable,
	}).Info("Latest StreamARN")
	return *streamArn, nil
}

func (app *appConfig) shardSyncStart(key primaryKey,
		streamArn string,
		shard *dynamodbstreams.Shard) {
	var iterator *dynamodbstreams.GetShardIteratorOutput
	var records *dynamodbstreams.GetRecordsOutput
	var err error

	parentShardId := shard.ParentShardId
	shardId := shard.ShardId
	// process parent shard before child
	if parentShardId != nil {
		for !app.isShardProcessed(key, parentShardId) {
			app.logger.WithFields(logging.Fields{
				"Shard Id":    *shardId,
				"Parent Shard Id":   parentShardId,
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
			}).Debug("Waiting for parent shard to complete")
			time.Sleep(time.Duration(shardWaitForParentIntervalSeconds))
		}
		app.logger.WithFields(logging.Fields{
			"Shard Id":    *shardId,
			"Parent Shard Id":   parentShardId,
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Debug("Completed processing parent shard")
	}

	shardIteratorInput := app.getShardIteratorInput(key, *shardId, streamArn)
	maxConnectRetries := app.sync[key].MaxConnectRetries

	for i := 0; i < maxConnectRetries; i++ {
		iterator, err = app.sync[key].stream.GetShardIterator(shardIteratorInput)
		if err != nil {
			if i == maxConnectRetries-1 {
				app.logger.WithFields(logging.Fields{
					"Shard Id":           *shardId,
					"Error":             err,
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
				}).Error("GetShardIterator Error")
				return
			}
			app.backoff(i, "GetShardIterator")
		} else {
			break
		}
	}

	shardIterator := iterator.ShardIterator

	// when nil, the shard has been closed, and the requested iterator
	// will not return any more data
	for shardIterator != nil {
		for i := 0; i < maxConnectRetries; i++ {
			records, err = app.sync[key].stream.GetRecords(
				&dynamodbstreams.GetRecordsInput{ShardIterator: shardIterator})
			if err != nil {
				if i == maxConnectRetries-1 {
					app.logger.WithFields(logging.Fields{
						"Shard Id":          *shardId,
						"Error":             err,
						"Source Table":      key.sourceTable,
						"Destination Table": key.dstTable,
						"Iterator":          shardIterator,
					}).Error("GetRecords Error")
					return
				}
				app.backoff(i, "GetRecords")
			} else {
				break
			}
		}

		if len(records.Records) > 0 {
			app.logger.WithFields(logging.Fields{
				"Shard Id":          *shardId,
				"Records len":       len(records.Records),
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
			}).Info("Shard sync, writing records")
			app.writeRecords(records.Records, key, shard)
		}
		shardIterator = records.NextShardIterator
	}
	// Completed shard processing
	app.logger.WithFields(logging.Fields{
		"Shard Id":          *shardId,
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
	}).Debug("Shard Iterator returns nil")
	app.markShardCompleted(key, shardId)
}

// Iterate through the records
// Depending on the action needed (MODIFY/INSERT/DELETE)
// perform required action on the dst table
// Once every `updateCheckpointThreshold` number of records are written,
// update the checkpoint
func (app *appConfig) writeRecords(
	records []*dynamodbstreams.Record,
	key primaryKey, shard *dynamodbstreams.Shard) {
	var err error
	for _, r := range records {
		err = nil
		switch *r.EventName {
		case "MODIFY":
			// same as insert
			fallthrough
		case "INSERT":
			err = app.insertRecord(r.Dynamodb.NewImage, key)
		case "REMOVE":
			err = app.removeRecord(r.Dynamodb.Keys, key)
		default:
			app.logger.WithFields(logging.Fields{
				"Event":             *r.EventName,
				"Record":            *r.Dynamodb,
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
				"Shard Id":          *shard.ShardId,
			}).Error("Unknown event on record")
		}

		if err != nil {
			app.logger.WithFields(logging.Fields{
				"Record":            *r.Dynamodb,
				"Event":             *r.EventName,
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
				"Shard Id":          *shard.ShardId,
				"Error":             err,
			}).Error("Failed to handle event")
		} else {
			app.logger.WithFields(logging.Fields{
				"Record":            *r.Dynamodb,
				"Event":             *r.EventName,
				"Source Table":      key.sourceTable,
				"Shard Id":          *shard.ShardId,
				"Destination Table": key.dstTable,
			}).Debug("Handled event successfully")
			lock := app.sync[key].checkpointLock
			lock.Lock()
			streamConf := app.sync[key]
			streamConf.recordCounter++
			app.sync[key] = streamConf
			app.logger.WithFields(logging.Fields{
				"Counter":           app.sync[key].recordCounter,
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
				"Shard Id":          *shard.ShardId,
			}).Info("Record counter")
			if app.sync[key].recordCounter == app.sync[key].UpdateCheckpointThreshold {
				app.updateCheckpoint(key,
					*r.Dynamodb.SequenceNumber,
					shard)
				// reset the recordCounter
				streamConf = app.sync[key]
				streamConf.recordCounter = 0
				app.sync[key] = streamConf
			}
			lock.Unlock()
		}
	}
}

// Insert this record in the dst table
func (app *appConfig) insertRecord(item map[string]*dynamodb.AttributeValue, key primaryKey) error {
	var err error
	maxConnectRetries := app.sync[key].MaxConnectRetries

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(app.sync[key].DstTable),
	}
	for i := 0; i < maxConnectRetries; i++ {
		_, err = app.sync[key].dstDynamo.PutItem(input)
		if err == nil {
			return nil
		} else {
			app.backoff(i, "PutItem")
		}
	}
	return err
}

// Remove this record from the dst table
func (app *appConfig) removeRecord(item map[string]*dynamodb.AttributeValue, key primaryKey) error {
	var err error
	maxConnectRetries := app.sync[key].MaxConnectRetries

	input := &dynamodb.DeleteItemInput{
		Key:       item,
		TableName: aws.String(app.sync[key].DstTable),
	}
	for i := 0; i < maxConnectRetries; i++ {
		_, err = app.sync[key].dstDynamo.DeleteItem(input)
		if err == nil {
			return nil
		} else {
			app.backoff(i, "DeleteItem")
		}
	}
	return err
}
