package main

import (
	"math"
	"time"

	logging "github.com/sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

func (app *appConfig) backoff(i int, s string) {
	wait := math.Pow(2, float64(i))
	app.logger.WithFields(logging.Fields{
		"Backoff Caller":             s,
		"Backoff Time(milliseconds)": wait,
	}).Info("Backing off")
	time.Sleep(time.Duration(wait) * time.Millisecond)
}

// Check if a shard is already processed
func (app *appConfig) isShardProcessed(key primaryKey, shardId *string) bool {
	lock := app.sync[key].activeShardLock
	lock.RLock()
	app.logger.WithFields(logging.Fields{
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
		"Shard Id":          *shardId,
	}).Debug("Checking if shard processing is completed")
	_, ok := app.sync[key].activeShardProcessors[*shardId]
	lock.RUnlock()
	if !ok {
		app.logger.WithFields(logging.Fields{
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
			"Shard Id":          *shardId,
		}).Debug("Shard processing complete")
	} else {
		app.logger.WithFields(logging.Fields{
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
			"Shard Id":          *shardId,
		}).Debug("Shard processing not yet complete")
	}

	return !ok
}

// Mark a shardId as completed
func (app *appConfig) markShardCompleted(key primaryKey, shardId *string) {
	app.expireCheckpointLocal(key, shardId)
	app.expireCheckpointRemote(key, *shardId)
}

// TRIM_HORIZON indicates we want to read from the beginning of the shard
// AFTER_SEQUENCE_NUMBER, and providing a sequence number (from the checkpoint
// allows us to read the shard from that point
func (app *appConfig) getShardIteratorInput(
	key primaryKey, shardId string,
	streamArn string) (*dynamodbstreams.GetShardIteratorInput) {
	var shardIteratorInput *dynamodbstreams.GetShardIteratorInput
	_, ok := app.state[key].checkpoint[shardId]
	if !ok {
		// New shard
		shardIteratorInput = &dynamodbstreams.GetShardIteratorInput{
			ShardId:           aws.String(shardId),
			ShardIteratorType: aws.String("TRIM_HORIZON"),
			StreamArn:         aws.String(streamArn),
		}
	} else {
		// Shard partially processed. Need to continue from the point where we left off
		shardIteratorInput = &dynamodbstreams.GetShardIteratorInput{
			ShardId:           aws.String(shardId),
			SequenceNumber:    aws.String(app.state[key].checkpoint[shardId]),
			ShardIteratorType: aws.String(shardIteratorPointer),
			StreamArn:         aws.String(streamArn),
		}
	}

	app.logger.WithFields(logging.Fields{
		"Shard Id":          shardId,
		"sharditeratorType": shardIteratorInput.ShardIteratorType,
		"StreamARN":         streamArn,
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
	}).Debug("ShardIterator")

	return shardIteratorInput
}
