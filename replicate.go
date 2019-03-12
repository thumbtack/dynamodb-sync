package main

import (
	"github.com/thumbtack/goratelimit"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	logging "github.com/sirupsen/logrus"
)

const (
	maxBatchSize               = 25
	streamRetentionHours       = 24 * time.Hour
	shardEnumerateInterval     = 300 * time.Second
	shardWaitForParentInterval = 60 * time.Second
	streamEnableWaitInterval   = 10 * time.Second
	shardIteratorPointer       = "AFTER_SEQUENCE_NUMBER"
)

func (sync *syncState) replicate(quit <-chan bool, key primaryKey) {
	// Check if we need to copy the table over
	// from src to dst before processing the streams
	if sync.isFreshStart(key) {
		sync.checkpoint = make(map[string]string, 0)
		sync.expiredShards = make(map[string]bool, 0)
		sync.timestamp = time.Time{}

		// Copy table start
		sync.copyTable(key)

		// Update checkpoint
		sync.updateCheckpointTimestamp(key)
	}

	// Check if streaming is needed
	if !sync.tableConfig.EnableStreaming {
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
			err := sync.streamSyncStart(key)
			if err != nil {
				logger.WithFields(logging.Fields{
					"Error":             err,
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
				}).Error("Error in replicating streams.")
				// Stream may not have been enabled. Wait before trying again
				time.Sleep(time.Duration(streamEnableWaitInterval))
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
func (state *syncState) copyTable(key primaryKey) {
	logger.WithFields(logging.Fields{
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
	}).Info("Copying dynamodb tables")

	// Fix up the r/w capacity of src and dst tables
	// Save the old values to reset the values once
	// we are done copying
	sourceCapacity := state.getCapacity(state.tableConfig.SrcTable, state.srcDynamo)
	dstCapacity := state.getCapacity(state.tableConfig.DstTable, state.dstDynamo)
	var isSourceThroughputChanged = false
	var isDstThroughputChanged = false
	srcDynamo := state.srcDynamo
	dstDynamo := state.dstDynamo

	if state.tableConfig.ReadQps > sourceCapacity.readCapacity {
		newCapacity := provisionedThroughput{
			state.tableConfig.ReadQps,
			sourceCapacity.writeCapacity,
		}
		err := state.updateCapacity(state.tableConfig.SrcTable, newCapacity, srcDynamo)
		if err != nil {
			logger.WithFields(logging.Fields{"Table": key.sourceTable}).
				Error("Unable to update capacity, won't proceed with copy")
			return
		}
		isSourceThroughputChanged = true
	}

	if state.tableConfig.WriteQps > dstCapacity.writeCapacity {
		newCapacity := provisionedThroughput{
			dstCapacity.readCapacity,
			state.tableConfig.WriteQps,
		}
		err := state.updateCapacity(state.tableConfig.DstTable, newCapacity, dstDynamo)
		if err != nil {
			logger.WithFields(logging.Fields{"Table": key.dstTable}).
				Error("Unable to update capacity, won't proceed with copy")
		}
		isDstThroughputChanged = true
	}

	var writerWG, readerWG sync.WaitGroup
	items := make(chan []map[string]*dynamodb.AttributeValue)
	writeWorkers := state.tableConfig.WriteWorkers
	readWorkers := state.tableConfig.ReadWorkers

	writerWG.Add(writeWorkers)
	rlDst := goratelimit.New(state.tableConfig.WriteQps)
	for i := 0; i < writeWorkers; i++ {
		logger.WithFields(logging.Fields{
			"Write Worker":      i,
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Debug("Starting copy table write worker..")
		go state.writeTable(key, items, &writerWG, i, *rlDst)
	}
	readerWG.Add(readWorkers)
	for i := 0; i < readWorkers; i++ {
		logger.WithFields(logging.Fields{
			"Read Worker":       i,
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Debug("Starting copy table read worker..")
		go state.readTable(key, items, &readerWG, i)
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
		err := state.updateCapacity(state.tableConfig.SrcTable, sourceCapacity, srcDynamo)
		if err != nil {
			logger.WithFields(logging.Fields{"Table": key.sourceTable}).
				Error("Failed to reset capacity")
		}
	}
	if isDstThroughputChanged {
		err := state.updateCapacity(state.tableConfig.DstTable, dstCapacity, dstDynamo)
		if err != nil {
			logger.WithFields(logging.Fields{"Table": key.dstTable}).
				Error("Failed to reset capacity")
		}
	}
}

// Check if the stream needs to be synced from the beginning, or
// from a particular checkpoint
func (sync *syncState) streamSyncStart(key primaryKey) error {
	logger.WithFields(logging.Fields{
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
	}).Info("Starting DynamoDB Stream Sync")
	var err error
	streamArn, err := sync.getStreamArn(key)
	if err != nil {
		return err
	}
	err = sync.streamSync(key, streamArn)

	return err
}

// Read from src stream, and write to dst table
func (sync *syncState) streamSync(key primaryKey, streamArn string) error {
	var err error = nil
	var result *dynamodbstreams.DescribeStreamOutput
	lastEvaluatedShardId := ""
	numShards := 0

	type shardStats struct {
		numShards    int
		tableName    string
	}

	type activeShardStats struct {
		numActiveShards int
		tableName       string
	}

	for {
		input := &dynamodbstreams.DescribeStreamInput{
			StreamArn: aws.String(streamArn),
			Limit:     aws.Int64(100),
		}

		if lastEvaluatedShardId != "" {
			input.ExclusiveStartShardId = aws.String(lastEvaluatedShardId)
		}
		maxConnectRetries := sync.tableConfig.MaxConnectRetries

		for i := 0; i < maxConnectRetries; i++ {
			result, err = sync.stream.DescribeStream(input)
			if err != nil {
				if i == maxConnectRetries-1 {
					return err
				}
				backoff(i, "Describe Stream")
			} else {
				break
			}
		}

		numShards += len(result.StreamDescription.Shards)
		
		for _, shard := range result.StreamDescription.Shards {
			sync.checkpointLock.RLock()
			_, ok := sync.expiredShards[*shard.ShardId]
			sync.checkpointLock.RUnlock()
			if ok {
				// 	Case 1: Shard has been processed in an earlier run
				logger.WithFields(logging.Fields{
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
					"Shard Id":          *shard.ShardId,
				}).Debug("Shard processed in an earlier run")
				continue
			}

			sync.activeShardLock.Lock()
			_, ok = sync.activeShardProcessors[*shard.ShardId]
			sync.activeShardLock.Unlock()

			if !ok {
				// Case 2: New shard - start processing
				logger.WithFields(logging.Fields{
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
					"Shard Id":          *shard.ShardId,
				}).Debug("Starting processor for shard")

				sync.activeShardLock.Lock()
				sync.activeShardProcessors[*shard.ShardId] = true
				sync.activeShardLock.Unlock()

				go sync.shardSyncStart(key, streamArn, shard)
			} else {
				// Case 3: Shard is currently being processed
				logger.WithFields(logging.Fields{
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
					"Shard Id":          *shard.ShardId,
				}).Debug("Shard is already being processed")
			}
		}

		metricsClient.Measure(activeShardStats{
			len(sync.activeShardProcessors),
			key.dstTable},
		)

		if result.StreamDescription.LastEvaluatedShardId != nil {
			lastEvaluatedShardId = *result.StreamDescription.LastEvaluatedShardId
		} else {
			// No more shards to be processed for now
			// wait a few seconds before trying again
			// This API cannot be called more than 10/s
			lastEvaluatedShardId = ""

			metricsClient.Measure(shardStats{numShards:numShards, tableName:key.sourceTable})

			numShards = 0
			logger.WithFields(logging.Fields{
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
				"SleepTime":         shardEnumerateInterval,
			}).Debug("Sleeping before refreshing list of shards")

			time.Sleep(time.Duration(shardEnumerateInterval))
		}
	}
	return err
}
