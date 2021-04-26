package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	logging "github.com/sirupsen/logrus"
)

// Check if a shard is already processed
func (ss *syncState) isShardProcessed(shardId *string) bool {
	ss.activeShardLock.RLock()
	_, ok := ss.activeShardProcessors[*shardId]
	ss.activeShardLock.RUnlock()

	logFields := logging.Fields{
		"Source Table":      ss.checkpointPK.sourceTable,
		"Destination Table": ss.checkpointPK.dstTable,
		"Shard Id":          *shardId,
	}
	logger.WithFields(logFields).Debug("Checking if shard processing is completed")
	if !ok {
		logger.WithFields(logFields).Debug("Shard processing complete")
	} else {
		logger.WithFields(logFields).Debug("Shard processing not yet complete")
	}
	return !ok
}

// TRIM_HORIZON indicates we want to read from the beginning of the shard
// AFTER_SEQUENCE_NUMBER, and providing a sequence number (from the checkpoint
// allows us to read the shard from that point
func (ss *syncState) getShardIteratorInput(
	shardId, streamArn string,
) *dynamodbstreams.GetShardIteratorInput {
	ss.checkpointLock.RLock()
	_, ok := ss.checkpoint[shardId]
	ss.checkpointLock.RUnlock()

	var shardIteratorInput *dynamodbstreams.GetShardIteratorInput
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
			SequenceNumber:    aws.String(ss.checkpoint[shardId]),
			ShardIteratorType: aws.String(shardIteratorPointer),
			StreamArn:         aws.String(streamArn),
		}
	}

	logger.WithFields(logging.Fields{
		"Shard Id":          shardId,
		"shardIteratorType": shardIteratorInput.ShardIteratorType,
		"StreamARN":         streamArn,
		"Source Table":      ss.checkpointPK.sourceTable,
		"Destination Table": ss.checkpointPK.dstTable,
	}).Debug("ShardIterator")

	return shardIteratorInput
}

// Mark a shardId as completed
func (ss *syncState) markShardCompleted(shardId *string) {
	ss.expireCheckpointLocal(shardId)
	ss.expireCheckpointRemote(*shardId)
}
