package main

import (
	"math"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	logging "github.com/sirupsen/logrus"
)

func backoff(i int, s string) {
	wait := math.Pow(2, float64(i))
	logger.WithFields(logging.Fields{
		"Backoff Caller":             s,
		"Backoff Time(seconds)": wait,
	}).Info("Backing off")
	time.Sleep(time.Duration(wait) * time.Second)
}

// Check if a shard is already processed
func (sync *syncState) isShardProcessed(key primaryKey, shardId *string) bool {
	sync.activeShardLock.RLock()
	logger.WithFields(logging.Fields{
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
		"Shard Id":          *shardId,
	}).Debug("Checking if shard processing is completed")
	_, ok := sync.activeShardProcessors[*shardId]
	if !ok {
		logger.WithFields(logging.Fields{
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
			"Shard Id":          *shardId,
		}).Debug("Shard processing complete")
	} else {
		logger.WithFields(logging.Fields{
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
			"Shard Id":          *shardId,
		}).Debug("Shard processing not yet complete")
	}
	sync.activeShardLock.RUnlock()
	return !ok
}

// Mark a shardId as completed
func (sync *syncState) markShardCompleted(key primaryKey, shardId *string) {
	sync.expireCheckpointLocal(key, shardId)
	sync.expireCheckpointRemote(key, *shardId)
}

// TRIM_HORIZON indicates we want to read from the beginning of the shard
// AFTER_SEQUENCE_NUMBER, and providing a sequence number (from the checkpoint
// allows us to read the shard from that point
func (sync *syncState) getShardIteratorInput(
	key primaryKey, shardId string,
	streamArn string) *dynamodbstreams.GetShardIteratorInput {
	var shardIteratorInput *dynamodbstreams.GetShardIteratorInput
	sync.checkpointLock.RLock()
	_, ok := sync.checkpoint[shardId]
	sync.checkpointLock.RUnlock()
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
			SequenceNumber:    aws.String(sync.checkpoint[shardId]),
			ShardIteratorType: aws.String(shardIteratorPointer),
			StreamArn:         aws.String(streamArn),
		}
	}

	logger.WithFields(logging.Fields{
		"Shard Id":          shardId,
		"sharditeratorType": shardIteratorInput.ShardIteratorType,
		"StreamARN":         streamArn,
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
	}).Debug("ShardIterator")

	return shardIteratorInput
}
