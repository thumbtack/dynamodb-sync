package main

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	logging "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

const (
	maxBatchSize               = 25
	streamRetentionHours       = 24 * time.Hour
	shardEnumerateInterval     = 5 * time.Minute
	shardWaitForParentInterval = time.Minute
	streamEnableWaitInterval   = 10 * time.Second
	shardIteratorPointer       = "AFTER_SEQUENCE_NUMBER"
)

func (ss *syncState) replicate(quit <-chan bool, key primaryKey) {
	// Check if we need to copy the table over from src to dst before processing the streams
	if ss.isFreshStart(key) {
		ss.checkpoint = map[string]string{}
		ss.expiredShards = map[string]bool{}
		ss.timestamp = time.Time{}

		// Copy table start
		if err := ss.copyTable(key); err != nil {
			return
		}
		// Update checkpoint
		ss.updateCheckpointTimestamp(key)
	}

	// Check if streaming is needed
	if !*ss.tableConfig.EnableStreaming {
		return
	}

	for {
		select {
		case <-quit:
			logger.WithFields(logging.Fields{
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
			}).Info("Quitting stream syncing")
			return
		default:
			// Start processing stream
			err := ss.streamSyncStart(key)
			if err != nil {
				logger.WithFields(logging.Fields{
					"Error":             err,
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
				}).Error("Error in replicating streams.")
				// Stream may not have been enabled. Wait before trying again
				time.Sleep(streamEnableWaitInterval)
			}
		}
	}
}

// Copy table from src to dst
// Read workers read from src table and write to a channel
// Write workers read the items from the channel and write
// to the dst table
// Once all the workers in the readWorker group are done,
// we close the channel, and wait for the writeWorker group
// to finish
func (ss *syncState) copyTable(key primaryKey) error {
	logger.WithFields(logging.Fields{
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
	}).Info("Copying dynamodb tables")

	// Fix up the r/w capacity of src and dst tables
	// Save the old values to reset the values once we are done copying
	isSourceThroughputChanged, isDstThroughputChanged := false, false
	sourceCapacity := ss.getCapacity(ss.tableConfig.SrcTable, ss.srcDynamo)
	dstCapacity := ss.getCapacity(ss.tableConfig.DstTable, ss.dstDynamo)
	srcDynamo := ss.srcDynamo
	dstDynamo := ss.dstDynamo

	if sourceCapacity.readCapacity != 0 && ss.tableConfig.ReadQPS > sourceCapacity.readCapacity {
		newCapacity := provisionedThroughput{
			ss.tableConfig.ReadQPS,
			sourceCapacity.writeCapacity,
		}
		if err := ss.updateCapacity(ss.tableConfig.SrcTable, newCapacity, srcDynamo); err != nil {
			logger.WithFields(logging.Fields{"Table": key.sourceTable}).
				Error("Unable to update capacity, won't proceed with copy")
			return err
		}
		isSourceThroughputChanged = true
	}

	if dstCapacity.writeCapacity != 0 && ss.tableConfig.WriteQPS > dstCapacity.writeCapacity {
		newCapacity := provisionedThroughput{
			dstCapacity.readCapacity,
			ss.tableConfig.WriteQPS,
		}
		if err := ss.updateCapacity(ss.tableConfig.DstTable, newCapacity, dstDynamo); err != nil {
			logger.WithFields(logging.Fields{"Table": key.dstTable}).
				Error("Unable to update capacity, won't proceed with copy")
			return err
		}
		isDstThroughputChanged = true
	}

	var writerWG, readerWG sync.WaitGroup
	items := make(chan []map[string]*dynamodb.AttributeValue)
	writeWorkers := ss.tableConfig.WriteWorkers
	readWorkers := ss.tableConfig.ReadWorkers

	writerWG.Add(writeWorkers)

	rl := rate.NewLimiter(rate.Limit(ss.tableConfig.WriteQPS), int(ss.tableConfig.WriteQPS))
	for i := 0; i < writeWorkers; i++ {
		logger.WithFields(logging.Fields{
			"Write Worker":      i,
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Debug("Starting copy table write worker..")
		go ss.writeTable(key, items, &writerWG, i, rl)
	}
	readerWG.Add(readWorkers)
	for i := 0; i < readWorkers; i++ {
		logger.WithFields(logging.Fields{
			"Read Worker":       i,
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Debug("Starting copy table read worker..")
		go ss.readTable(key, items, &readerWG, i)
	}

	readerWG.Wait()
	close(items)
	writerWG.Wait()

	logger.WithFields(logging.Fields{
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
	}).Info("Finished copying dynamodb tables")

	// Reset table capacity to original values
	if isSourceThroughputChanged {
		err := ss.updateCapacity(ss.tableConfig.SrcTable, sourceCapacity, srcDynamo)
		if err != nil {
			logger.Errorf("Failed to reset capacity for table: %s", key.sourceTable)
		}
	}
	if isDstThroughputChanged {
		err := ss.updateCapacity(ss.tableConfig.DstTable, dstCapacity, dstDynamo)
		if err != nil {
			logger.Errorf("Failed to reset capacity for table: %s", key.dstTable)
		}
	}
	return nil
}

// Check if the stream needs to be synced from the beginning, or
// from a particular checkpoint
func (ss *syncState) streamSyncStart(key primaryKey) error {
	logger.WithFields(logging.Fields{
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
	}).Info("Starting DynamoDB Stream Sync")
	streamArn, err := ss.getStreamArn(key)
	if err != nil {
		return err
	}
	return ss.streamSync(key, streamArn)
}

// Read from src stream, and write to dst table
func (ss *syncState) streamSync(key primaryKey, streamArn string) (err error) {
	var result *dynamodbstreams.DescribeStreamOutput
	lastEvaluatedShardId := ""
	numShards := 0

	for {
		input := &dynamodbstreams.DescribeStreamInput{
			StreamArn: aws.String(streamArn),
			Limit:     aws.Int64(100),
		}

		if lastEvaluatedShardId != "" {
			input.ExclusiveStartShardId = aws.String(lastEvaluatedShardId)
		}

		for i := 1; i <= maxRetries; i++ {
			result, err = ss.stream.DescribeStream(input)
			if err == nil {
				break
			}
			if i == maxRetries {
				return err
			}
			backoff(i, "Describe Stream")
		}

		numShards += len(result.StreamDescription.Shards)

		for _, shard := range result.StreamDescription.Shards {
			ss.checkpointLock.RLock()
			_, ok := ss.expiredShards[*shard.ShardId]
			ss.checkpointLock.RUnlock()
			if ok {
				// 	Case 1: Shard has been processed in an earlier run
				logger.WithFields(logging.Fields{
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
					"Shard Id":          *shard.ShardId,
				}).Debug("Shard processed in an earlier run")
				continue
			}

			ss.activeShardLock.Lock()
			_, ok = ss.activeShardProcessors[*shard.ShardId]
			ss.activeShardLock.Unlock()

			if !ok {
				// Case 2: New shard - start processing
				logger.WithFields(logging.Fields{
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
					"Shard Id":          *shard.ShardId,
				}).Debug("Starting processor for shard")

				ss.activeShardLock.Lock()
				ss.activeShardProcessors[*shard.ShardId] = true
				ss.activeShardLock.Unlock()

				go ss.shardSyncStart(key, streamArn, shard)
			} else {
				// Case 3: Shard is currently being processed
				logger.WithFields(logging.Fields{
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
					"Shard Id":          *shard.ShardId,
				}).Debug("Shard is already being processed")
			}
		}

		if result.StreamDescription.LastEvaluatedShardId != nil {
			lastEvaluatedShardId = *result.StreamDescription.LastEvaluatedShardId
		} else {
			// No more shards to be processed for now
			// wait a few seconds before trying again
			// This API cannot be called more than 10/s
			lastEvaluatedShardId = ""

			numShards = 0
			logger.WithFields(logging.Fields{
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
				"SleepTime":         shardEnumerateInterval,
			}).Debug("Sleeping before refreshing list of shards")

			time.Sleep(shardEnumerateInterval)
		}
	}
}
