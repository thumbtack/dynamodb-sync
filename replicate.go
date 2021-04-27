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

// If the state has no timestamp, or if the timestamp
// is more than 24 hours old, returns True. Else, False
func (ss *syncState) isFreshStart() bool {
	logger.WithFields(logging.Fields{
		"src table":    ss.checkpointPK.sourceTable,
		"dst table":    ss.checkpointPK.dstTable,
		"State Timestamp":   ss.timestamp,
	}).Info("Checking if fresh start")
	return ss.timestamp.IsZero() || time.Now().Sub(ss.timestamp) > streamRetentionHours
}

func (ss *syncState) replicate(quit <-chan bool) {
	// Check if we need to copy the table over from src to dst before processing the streams
	if ss.isFreshStart() {
		ss.checkpoint = map[string]string{}
		ss.expiredShards = map[string]bool{}
		ss.timestamp = time.Time{}

		// Copy table start
		if err := ss.copyTable(); err != nil {
			return
		}
		// Update checkpoint
		ss.updateCheckpointTimestamp()
	}

	// Check if streaming is needed
	if !*ss.tableConfig.EnableStreaming {
		return
	}

	for {
		select {
		case <-quit:
			logger.WithFields(logging.Fields{
				"Source Table":      ss.checkpointPK.sourceTable,
				"Destination Table": ss.checkpointPK.dstTable,
			}).Info("Quitting stream syncing")
			return
		default:
			// Start processing stream
			err := ss.streamSyncStart()
			if err != nil {
				logger.WithFields(logging.Fields{
					"Error":             err,
					"Source Table":      ss.checkpointPK.sourceTable,
					"Destination Table": ss.checkpointPK.dstTable,
				}).Error("Error in replicating streams.")
				// Stream may not have been enabled. Wait before trying again
				time.Sleep(streamEnableWaitInterval)
			}
		}
	}
}

// copyTable copies the items from the source table to the destination table.
// The read workers read the items from source table and write to a channel.
// The write workers read the items from the channel and write to the destination table.
// Once all the workers in the readWorker group are done, the channel will be closed and waiting for
// the writeWorker group to finish.
func (ss *syncState) copyTable() error {
	logger.WithFields(logging.Fields{
		"src table": ss.checkpointPK.sourceTable,
		"dst table": ss.checkpointPK.dstTable,
	}).Info("copying tables")

	srcCapacity, _ := getCapacity(ss.tableConfig.SrcTable, ss.srcDynamo)
	// bump the capacity for the source table if the billing mode is provisioned
	if srcCapacity.readCapacity != 0 {
		newCapacity := provisionedThroughput{
			readCapacity:  srcCapacity.readCapacity + ss.tableConfig.ReadQPS,
			writeCapacity: srcCapacity.writeCapacity,
		}
		if err := updateCapacity(ss.tableConfig.SrcTable, newCapacity, ss.srcDynamo); err != nil {
			logger.WithFields(logging.Fields{
				"table": ss.checkpointPK.sourceTable,
			}).Error("failed to update src table capacity, stop proceeding")
			return err
		}
		defer func() {
			err := updateCapacity(ss.tableConfig.SrcTable, *srcCapacity, ss.srcDynamo)
			if err != nil {
				logger.Errorf("failed to reset capacity for %s", ss.checkpointPK.sourceTable)
			}
		}()
	}

	dstCapacity, _ := getCapacity(ss.tableConfig.DstTable, ss.dstDynamo)
	// bump the capacity for the destination table if the billing mode is provisioned
	if dstCapacity.writeCapacity != 0 {
		newCapacity := provisionedThroughput{
			readCapacity:  dstCapacity.readCapacity,
			writeCapacity: dstCapacity.writeCapacity + ss.tableConfig.WriteQPS,
		}
		if err := updateCapacity(ss.tableConfig.DstTable, newCapacity, ss.dstDynamo); err != nil {
			logger.WithFields(logging.Fields{
				"table": ss.checkpointPK.dstTable,
			}).Error("failed to update dst table capacity, stop proceeding")
			return err
		}
		defer func() {
			err := updateCapacity(ss.tableConfig.DstTable, *dstCapacity, ss.dstDynamo)
			if err != nil {
				logger.Errorf("failed to reset capacity for %s", ss.checkpointPK.dstTable)
			}
		}()
	}

	var readerWG, writerWG sync.WaitGroup
	items := make(chan []map[string]*dynamodb.AttributeValue)

	writerWG.Add(ss.tableConfig.WriteWorkers)
	rl := rate.NewLimiter(rate.Limit(ss.tableConfig.WriteQPS), int(ss.tableConfig.WriteQPS))
	for i := 0; i < ss.tableConfig.WriteWorkers; i++ {
		logger.WithFields(logging.Fields{
			"Write Worker": i,
			"src table":    ss.checkpointPK.sourceTable,
			"dst table":    ss.checkpointPK.dstTable,
		}).Debug("Starting copy table write worker..")
		go ss.writeTable(items, &writerWG, i, rl)
	}
	readerWG.Add(ss.tableConfig.ReadWorkers)
	for i := 0; i < ss.tableConfig.ReadWorkers; i++ {
		logger.WithFields(logging.Fields{
			"Read Worker": i,
			"src table":   ss.checkpointPK.sourceTable,
			"dst table":   ss.checkpointPK.dstTable,
		}).Debug("Starting copy table read worker..")
		go ss.readTable(items, &readerWG, i)
	}

	readerWG.Wait()
	close(items)
	writerWG.Wait()

	logger.WithFields(logging.Fields{
		"src table": ss.checkpointPK.sourceTable,
		"dst table": ss.checkpointPK.dstTable,
	}).Info("finished copying tables")
	return nil
}

// Check if the stream needs to be synced from the beginning, or
// from a particular checkpoint
func (ss *syncState) streamSyncStart() error {
	logger.WithFields(logging.Fields{
		"src table": ss.checkpointPK.sourceTable,
		"dst table": ss.checkpointPK.dstTable,
	}).Info("Starting DynamoDB Stream Sync")
	streamArn, err := ss.getStreamArn()
	if err != nil {
		return err
	}
	return ss.streamSync(streamArn)
}

// Read from src stream, and write to dst table
func (ss *syncState) streamSync(streamArn string) (err error) {
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
			backoff(i)
		}

		numShards += len(result.StreamDescription.Shards)

		for _, shard := range result.StreamDescription.Shards {
			ss.checkpointLock.RLock()
			_, ok := ss.expiredShards[*shard.ShardId]
			ss.checkpointLock.RUnlock()
			if ok {
				// 	Case 1: Shard has been processed in an earlier run
				logger.WithFields(logging.Fields{
					"src table": ss.checkpointPK.sourceTable,
					"dst table": ss.checkpointPK.dstTable,
					"Shard Id":  *shard.ShardId,
				}).Debug("Shard processed in an earlier run")
				continue
			}

			ss.activeShardLock.Lock()
			_, ok = ss.activeShardProcessors[*shard.ShardId]
			ss.activeShardLock.Unlock()

			if !ok {
				// Case 2: New shard - start processing
				logger.WithFields(logging.Fields{
					"src table": ss.checkpointPK.sourceTable,
					"dst table": ss.checkpointPK.dstTable,
					"Shard Id":  *shard.ShardId,
				}).Debug("Starting processor for shard")

				ss.activeShardLock.Lock()
				ss.activeShardProcessors[*shard.ShardId] = true
				ss.activeShardLock.Unlock()

				go ss.shardSyncStart(streamArn, shard)
			} else {
				// Case 3: Shard is currently being processed
				logger.WithFields(logging.Fields{
					"src table": ss.checkpointPK.sourceTable,
					"dst table": ss.checkpointPK.dstTable,
					"Shard Id":  *shard.ShardId,
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
				"src table": ss.checkpointPK.sourceTable,
				"dst table": ss.checkpointPK.dstTable,
				"SleepTime": shardEnumerateInterval,
			}).Debug("Sleeping before refreshing list of shards")

			time.Sleep(shardEnumerateInterval)
		}
	}
}
