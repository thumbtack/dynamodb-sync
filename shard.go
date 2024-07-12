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
		"src_table": ss.checkpointPK.sourceTable,
		"dst_table": ss.checkpointPK.dstTable,
		"shard_id":  *shardId,
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

	shardIteratorInput := &dynamodbstreams.GetShardIteratorInput{
		ShardId:   aws.String(shardId),
		StreamArn: aws.String(streamArn),
	}
	if !ok {
		// New shard
		shardIteratorInput.SetShardIteratorType("TRIM_HORIZON")
	} else {
		// Shard partially processed. Need to continue from the point where we left off
		shardIteratorInput.SetShardIteratorType(shardIteratorPointer)
		shardIteratorInput.SetSequenceNumber(ss.checkpoint[shardId])
	}

	logger.WithFields(logging.Fields{
		"shard_id":            shardId,
		"shard_iterator_type": *shardIteratorInput.ShardIteratorType,
		"stream_arn":          streamArn,
		"src_table":           ss.checkpointPK.sourceTable,
		"dst_table":           ss.checkpointPK.dstTable,
	}).Debug("ShardIterator")

	return shardIteratorInput
}

// Mark a shardId as completed
func (ss *syncState) markShardCompleted(shardId *string) {
	ss.expireCheckpointLocal(shardId)
	ss.expireCheckpointRemote(*shardId)
}
