package main

import (
	"sync"
	"time"

	logging "github.com/sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

const (
	maxBatchSize                      = 25
	streamRetentionHours              = 24 * time.Hour
	shardEnumerateIntervalSeconds     = 30 * time.Second
	shardWaitForParentIntervalSeconds = 5 * time.Second
	streamEnableWaitIntervalSeconds   = 10 * time.Second
	shardIteratorPointer              = "AFTER_SEQUENCE_NUMBER"
)

func (app *appConfig) replicate(quit <-chan bool, key primaryKey) {
	// Check if we need to copy the table over
	// from src to dst before processing the streams
	if app.isFreshStart(key) {
		// If there are any stale checkpoints/expired shards
		// it's time to delete those from the checkpoint table
		app.dropCheckpoint(key)
		// reset its local `state`
		app.state[key] = *NewStateTracker()
		/*err := app.deleteTable(key)*/
		app.copyTable(key)
		app.updateCheckpointTimestamp(key)
	}
	for {
		select {
		case <-quit:
			app.logger.WithFields(logging.Fields{
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
			}).Info("Quitting stream syncing")
			return
		default:
			// Start processing stream
			err := app.streamSyncStart(key)
			if err != nil {
				app.logger.WithFields(logging.Fields{
					"Error":             err,
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
				}).Error("Error in replicating streams.")
				// Stream may not have been enabled. Wait before trying again
				time.Sleep(time.Duration(streamEnableWaitIntervalSeconds))
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
func (app *appConfig) copyTable(key primaryKey) {
	app.logger.WithFields(logging.Fields{
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
	}).Info("Copying dynamodb tables")
	var writerWG, readerWG sync.WaitGroup
	items := make(chan []map[string]*dynamodb.AttributeValue)
	writeWorkers := app.sync[key].WriteWorkers
	readWorkers := app.sync[key].ReadWorkers

	writerWG.Add(writeWorkers)
	for i := 0; i < writeWorkers; i++ {
		app.logger.WithFields(logging.Fields{
			"Write Worker":      i,
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Debug("Starting copy table write worker..")
		go app.writeTable(key, items, &writerWG, i)
	}
	readerWG.Add(readWorkers)
	for i := 0; i < readWorkers; i++ {
		app.logger.WithFields(logging.Fields{
			"Read Worker":       i,
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Debug("Starting copy table read worker..")
		go app.readTable(key, items, &readerWG, i)
	}

	readerWG.Wait()
	close(items)
	writerWG.Wait()

	app.logger.WithFields(logging.Fields{
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
	}).Info("Finished copying dynamodb tables")
}

func (app *appConfig) deleteTable(key primaryKey) {
	app.logger.WithFields(logging.Fields{
		"Destination Table": key.dstTable,
	}).Info("Dropping items from stale table")
	var writerWG, readerWG sync.WaitGroup
	items := make(chan []map[string]*dynamodb.AttributeValue)
	writeWorkers := app.sync[key].WriteWorkers
	readWorkers := app.sync[key].ReadWorkers

	writerWG.Add(writeWorkers)
	for i := 0; i < writeWorkers; i++ {
		app.logger.WithFields(logging.Fields{
			"Write Worker":      i,
			"Destination Table": key.dstTable,
		}).Debug("Starting delete table write worker..")
		go app.batchDeleteItems(key, items, &writerWG, i)
	}
	readerWG.Add(readWorkers)
	for i := 0; i < readWorkers; i++ {
		app.logger.WithFields(logging.Fields{
			"Read Worker":       i,
			"Destination Table": key.dstTable,
		}).Debug("Starting delete table read worker..")
		go app.readTable(key, items, &readerWG, i)
	}

	readerWG.Wait()
	close(items)
	writerWG.Wait()

	app.logger.WithFields(logging.Fields{
		"Destination Table": key.dstTable,
	}).Info("Finished dropping items from dynamodb table")
}

// Check if the stream needs to be synced from the beginning, or
// from a particular checkpoint
func (app *appConfig) streamSyncStart(key primaryKey) (error) {
	app.logger.WithFields(logging.Fields{
		"Source Table":      key.sourceTable,
		"Destination Table": key.dstTable,
	}).Info("Starting DynamoDB Stream Sync")
	var err error
	streamArn, err := app.getStreamArn(key)
	if err != nil {
		return err
	}
	err = app.streamSync(key, streamArn)

	return err
}

// Read from src stream, and write to dst table
func (app *appConfig) streamSync(key primaryKey, streamArn string) (error) {
	var err error = nil
	var result *dynamodbstreams.DescribeStreamOutput
	lastEvaluatedShardId := ""

	for {
		input := &dynamodbstreams.DescribeStreamInput{
			StreamArn: aws.String(streamArn),
			Limit:     aws.Int64(100),
		}

		if lastEvaluatedShardId != "" {
			input.ExclusiveStartShardId = aws.String(lastEvaluatedShardId)
		}
		maxConnectRetries := app.sync[key].MaxConnectRetries

		for i := 0; i < maxConnectRetries; i++ {
			result, err = app.sync[key].stream.DescribeStream(input)
			if err != nil {
				if i == maxConnectRetries-1 {
					return err
				}
				app.backoff(i, "Describe Stream")
			} else {
				break
			}
		}
		for _, shard := range result.StreamDescription.Shards {
			lock := app.sync[key].checkpointLock
			lock.RLock()
			_, ok := app.state[key].expiredShards[*shard.ShardId]
			lock.RUnlock()
			if ok {
				// 	Case 1: Shard has been processed in an earlier run
				app.logger.WithFields(logging.Fields{
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
					"Shard Id":          *shard.ShardId,
				}).Debug("Shard processed in an earlier run")
				continue
			}
			lock = app.sync[key].activeShardLock
			lock.Lock()
			_, ok = app.sync[key].activeShardProcessors[*shard.ShardId]
			if !ok {
				// Case 2: New shard - start processing
				app.logger.WithFields(logging.Fields{
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
					"Shard Id":          *shard.ShardId,
				}).Debug("Starting processor for shard")
				go app.shardSyncStart(key, streamArn, shard)
				app.sync[key].activeShardProcessors[*shard.ShardId] = true
			} else {
				// Case 3: Shard is currently being processed
				app.logger.WithFields(logging.Fields{
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
					"Shard Id":          *shard.ShardId,
				}).Debug("Shard is already being processed")
			}
			lock.Unlock()
		}

		if result.StreamDescription.LastEvaluatedShardId != nil {
			lastEvaluatedShardId = *result.StreamDescription.LastEvaluatedShardId
		} else {
			// No more shards to be processed for now
			// wait a few seconds before trying again
			// This API cannot be called more than 10/s
			lastEvaluatedShardId = ""
			app.logger.WithFields(logging.Fields{
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
				"SleepTime":         shardEnumerateIntervalSeconds,
			}).Debug("Sleeping before refreshing list of shards")
			time.Sleep(time.Duration(shardEnumerateIntervalSeconds))
		}
	}
	return err
}
