package main

import (
	"errors"
	"fmt"
	"os"
	"sync"

	logging "github.com/sirupsen/logrus"
	"github.com/thumbtack/goratelimit"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Maximum size of a batch can be 25 items
// Batch Writes item to dst table
func (app *appConfig) writeBatch(
	batch map[string][]*dynamodb.WriteRequest,
	key primaryKey) error {
	var err error
	maxConnectRetries := app.sync[key].MaxConnectRetries
	dst := app.sync[key].DstTable

	for len(batch) > 0 {
		for i := 0; i < maxConnectRetries; i++ {
			output, err := app.sync[key].dstDynamo.BatchWriteItem(
				&dynamodb.BatchWriteItemInput{
					RequestItems: batch,
				})
			if err != nil {
				if i == maxConnectRetries-1 {
					return err
				}
				app.logger.WithFields(logging.Fields{
					"Source Table":         key.sourceTable,
					"Destination Table":    key.dstTable,
					"BatchWrite Item Size": len(batch),
				}).Debug("BatchWrite size")
				app.backoff(i, "BatchWrite")
				continue
			}
			n := len(output.UnprocessedItems[dst])
			if n > 0 {
				app.logger.WithFields(logging.Fields{
					"Unprocessed Items Size": n,
					"Source Table":           key.sourceTable,
					"Destination Table":      key.dstTable,
				}).Debug("Some items failed to be processed")
				// Retry writing items that were not processed
				batch = output.UnprocessedItems
				// don't try connecting again, go back to main for loop
				break
			} else {
				// all done
				return nil
			}
		}
	}
	return errors.New(fmt.Sprintf(
		"BatchWrite failed after %d attempts: %s",
		maxConnectRetries, err.Error()),
	)
}

// Group items from the `items` channel into
// batches of 25 (max batch size allowed by AWS)
// Write this batch to the dst table
// If there are any more items left in the end,
// process those too
func (app *appConfig) writeTable(
	key primaryKey,
	itemsChan chan []map[string]*dynamodb.AttributeValue,
	writerWG *sync.WaitGroup, id int) {
	defer writerWG.Done()
	var writeBatchSize int64
	rl := goratelimit.New(app.sync[key].WriteQps)
	//rl.Debug(app.verbose)
	writeRequest := make(map[string][]*dynamodb.WriteRequest, 0)
	dst := app.sync[key].DstTable

	if maxBatchSize < app.sync[key].WriteQps {
		writeBatchSize = maxBatchSize
	} else {
		writeBatchSize = app.sync[key].WriteQps
	}

	for {
		items, more := <-itemsChan
		if !more {
			app.logger.WithFields(logging.Fields{
				"Write Worker":      id,
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
			}).Debug("Write worker has finished")
			return
		}

		for _, item := range items {
			requestSize := len(writeRequest[dst])
			if int64(requestSize) == writeBatchSize {
				rl.Acquire(key.dstTable, writeBatchSize)
				err := app.writeBatch(writeRequest, key)
				if err != nil {
					app.logger.WithFields(logging.Fields{
						"Error":             err,
						"Source Table":      key.sourceTable,
						"Destination Table": key.dstTable,
					}).Error("Failed to write batch")
				} else {
					lock := app.progress[key].writeCountLock
					lock.Lock()
					progress := app.progress[key]
					progress.totalWrites = progress.totalWrites + int64(requestSize)
					app.progress[key] = progress
					lock.Unlock()
					app.logger.WithFields(logging.Fields{
						"Write worker":      id,
						"Write items size":  requestSize,
						"Source Table":      key.sourceTable,
						"Total Writes":      progress.totalWrites,
						"Destination Table": key.dstTable,
					}).Debug("Successfully wrote to dynamodb table")
				}
				writeRequest[dst] = []*dynamodb.WriteRequest{{
					PutRequest: &dynamodb.PutRequest{
						Item: item,
					}}}
			} else {
				writeRequest[dst] = append(writeRequest[dst], &dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: item,
					}})
			}
		}

		// Maybe more items are left because len(items) % maxBatchSize != 0
		requestSize := len(writeRequest[dst])
		if requestSize > 0 {
			rl.Acquire(key.dstTable, int64(requestSize))
			err := app.writeBatch(writeRequest, key)
			if err != nil {
				app.logger.WithFields(logging.Fields{
					"Error":             err,
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
				}).Error("Failed to write batch")
			} else {
				app.logger.WithFields(logging.Fields{
					"Write worker":      id,
					"Write items size":  requestSize,
					"Source Table":      key.sourceTable,
					"Destination Table": key.dstTable,
				}).Debug("Successfully wrote to dynamodb table")
			}

			writeRequest = make(map[string][]*dynamodb.WriteRequest, 0)
		}
	}
}

// Scan the table, and put the items in the `items` channel
// It is not necessary that the entire table be scanned in a
// single call to `scan`
// So, we scan the table in a loop until the len(lastEvaluatedKey)
// is zero
func (app *appConfig) readTable(
	key primaryKey,
	items chan []map[string]*dynamodb.AttributeValue,
	readerWG *sync.WaitGroup,
	id int) {
	defer readerWG.Done()

	lastEvaluatedKey := make(map[string]*dynamodb.AttributeValue, 0)
	maxConnectRetries := app.sync[key].MaxConnectRetries
	rl := goratelimit.New(app.sync[key].ReadQps)
	//rl.Debug(app.verbose)

	for {
		input := &dynamodb.ScanInput{
			TableName:      aws.String(key.sourceTable),
			ConsistentRead: aws.Bool(true),
			Segment:        aws.Int64(int64(id)),
			TotalSegments:  aws.Int64(int64(app.sync[key].ReadWorkers)),
		}

		if len(lastEvaluatedKey) > 0 {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		successfulScan := false
		for i := 0; i < maxConnectRetries; i++ {
			rl.Acquire("", 1)
			result, err := app.sync[key].srcDynamo.Scan(input)
			if err != nil {
				app.logger.WithFields(logging.Fields{
					"Error":        err,
					"Source Table": key.sourceTable,
				}).Debug("Scan returned error")
				app.backoff(i, "Scan")
			} else {
				successfulScan = true
				lastEvaluatedKey = result.LastEvaluatedKey
				items <- result.Items
				lock := app.progress[key].readCountLock
				lock.Lock()
				progress := app.progress[key]
				progress.totalReads = progress.totalReads + *result.Count
				app.progress[key] = progress
				lock.Unlock()
				app.logger.WithFields(logging.Fields{
					"Scanned items size": len(result.Items),
					"Scanned Count":      result.ScannedCount,
					"Total Reads":        progress.totalReads,
					"LastEvaluatedKey":   lastEvaluatedKey,
					"Source Table":       key.sourceTable,
				}).Debug("Scan successful")
				break
			}
		}

		if successfulScan {
			if len(lastEvaluatedKey) == 0 {
				app.logger.WithFields(logging.Fields{
					"Source Table": key.sourceTable,
				}).Debug("Scan completed")
				return
			}
		} else {
			app.logger.WithFields(logging.Fields{
				"Source Table":       key.sourceTable,
				"Number of Attempts": maxConnectRetries,
			}).Error("Scan failed")
			os.Exit(1)
		}
	}
}

func (app *appConfig) batchDeleteItems(
	key primaryKey,
	items chan []map[string]*dynamodb.AttributeValue,
	writerWG *sync.WaitGroup,
	i int) (error) {
	defer writerWG.Done()
	return nil
}
