package main

import (
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	logging "github.com/sirupsen/logrus"
)

const (
	MB = 1000000
)

// Maximum size of a batch can be 25 items
// Batch Writes item to dst table
func (sync *syncState) writeBatch(
	batch map[string][]*dynamodb.WriteRequest,
	key primaryKey, rl *rate.Limiter, reqCapacity float64,
	writeBatchSize int64) []*dynamodb.ConsumedCapacity {
	i := 0
	r := rl.ReserveN(time.Now(), int(reqCapacity))
	if !r.OK() {
		r = rl.ReserveN(time.Now(), int(writeBatchSize))
	}
	time.Sleep(r.Delay())
	consumedCapacity := make([]*dynamodb.ConsumedCapacity, 0)

	for len(batch) > 0 {
		output, _ := sync.dstDynamo.BatchWriteItem(
			&dynamodb.BatchWriteItemInput{
				RequestItems: batch,
			})

		consumedCapacity = append(consumedCapacity, output.ConsumedCapacity...)

		if len(output.UnprocessedItems) != 0 {
			logger.WithFields(logging.Fields{
				"Unprocessed Items Size": len(output.UnprocessedItems),
				"Source Table":           key.sourceTable,
				"Destination Table":      key.dstTable,
			}).Debug("Some items failed to be processed")
			// exponential backoff before retrying
			backoff(i, "BatchWrite")
			i++
			// Retry writing items that were not processed
			batch = output.UnprocessedItems
		}
	}
	return consumedCapacity
}

// Group items from the `items` channel into
// batches of 25 (max batch size allowed by AWS)
// Write this batch to the dst table
// If there are any more items left in the end,
// process those too
func (sync *syncState) writeTable(
	key primaryKey,
	itemsChan chan []map[string]*dynamodb.AttributeValue,
	writerWG *sync.WaitGroup, id int, rl *rate.Limiter) {
	defer writerWG.Done()
	var writeBatchSize int64
	var reqCapacity float64 = 0

	writeRequest := make(map[string][]*dynamodb.WriteRequest, 0)
	dst := sync.tableConfig.DstTable

	if maxBatchSize < sync.tableConfig.WriteQps {
		writeBatchSize = maxBatchSize
	} else {
		writeBatchSize = sync.tableConfig.WriteQps
	}

	for {
		items, more := <-itemsChan
		if !more {
			logger.WithFields(logging.Fields{
				"Write Worker":      id,
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
			}).Debug("Write worker has finished")
			return
		}

		for _, item := range items {
			requestSize := len(writeRequest[dst])
			if int64(requestSize) == writeBatchSize {
				consumedCapacity := sync.writeBatch(writeRequest, key, rl, reqCapacity, writeBatchSize)
				reqCapacity = 0
				for _, each := range consumedCapacity {
					reqCapacity += *each.CapacityUnits
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
			sync.writeBatch(writeRequest, key, rl, reqCapacity, writeBatchSize)
			writeRequest = make(map[string][]*dynamodb.WriteRequest, 0)
		}
	}
}

// Scan the table, and put the items in the `items` channel
// It is not necessary that the entire table be scanned in a
// single call to `scan`
// So, we scan the table in a loop until the len(lastEvaluatedKey)
// is zero
func (sync *syncState) readTable(
	key primaryKey,
	items chan []map[string]*dynamodb.AttributeValue,
	readerWG *sync.WaitGroup,
	id int) {
	defer readerWG.Done()

	lastEvaluatedKey := make(map[string]*dynamodb.AttributeValue, 0)

	for {
		input := &dynamodb.ScanInput{
			TableName:      aws.String(sync.tableConfig.SrcTable),
			ConsistentRead: aws.Bool(true),
			Segment:        aws.Int64(int64(id)),
			TotalSegments:  aws.Int64(int64(sync.tableConfig.ReadWorkers)),
		}

		if len(lastEvaluatedKey) > 0 {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		successfulScan := false

		for i := 0; i < maxRetries; i++ {
			result, err := sync.srcDynamo.Scan(input)
			if err != nil {
				logger.WithFields(logging.Fields{
					"Error":        err,
					"Source Table": key.sourceTable,
				}).Debug("Scan returned error")
				backoff(i, "Scan")
			} else {
				successfulScan = true
				lastEvaluatedKey = result.LastEvaluatedKey
				items <- result.Items
				logger.WithFields(logging.Fields{
					"Scanned items size": len(result.Items),
					"Scanned Count":      *result.ScannedCount,
					"LastEvaluatedKey":   lastEvaluatedKey,
					"Source Table":       key.sourceTable,
				}).Debug("Scan successful")
				break
			}
		}

		if successfulScan {
			if len(lastEvaluatedKey) == 0 {
				logger.WithFields(logging.Fields{
					"Source Table": key.sourceTable,
				}).Debug("Scan completed")
				return
			}
		} else {
			logger.WithFields(logging.Fields{
				"Source Table":       key.sourceTable,
				"Number of Attempts": maxRetries,
			}).Error("Scan failed")
			os.Exit(1)
		}
	}
}

func (sync *syncState) updateCapacity(
	tableName string,
	newThroughput provisionedThroughput,
	dynamo *dynamodb.DynamoDB) error {

	var err error
	logger.WithFields(logging.Fields{
		"Table":              tableName,
		"New Read Capacity":  newThroughput.readCapacity,
		"New Write Capacity": newThroughput.writeCapacity}).Info("Updating capacity")
	input := &dynamodb.UpdateTableInput{
		TableName: aws.String(tableName),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(newThroughput.readCapacity),
			WriteCapacityUnits: aws.Int64(newThroughput.writeCapacity),
		},
	}

	for i := 0; i < maxRetries; i++ {
		_, err = dynamo.UpdateTable(input)
		if err != nil {
			logger.WithFields(logging.Fields{
				"Error": err,
				"Table": tableName,
			}).Error("Error in updating table capacity")
		} else {
			break
		}
	}

	if err != nil {
		logger.WithFields(logging.Fields{
			"Error": err,
			"Table": tableName,
		}).Error("Failed to update table capacity")
		return err
	}

	// Wait for table to be updated
	status := ""
	statusInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	for status != "ACTIVE" {
		output, err := dynamo.DescribeTable(statusInput)
		if err != nil {
			logger.WithFields(logging.Fields{
				"Error": err,
				"Table": tableName,
			}).Error("Failed to get table status")
			break
		} else {
			status = *output.Table.TableStatus
			logger.WithFields(logging.Fields{
				"Table": tableName,
			}).Debug("Updating table throughput")
			time.Sleep(1 * time.Second)
		}
	}

	logger.WithFields(logging.Fields{
		"Table": tableName,
	}).Info("Successfully updated table throughput")

	return nil
}

// Since we are interested in the read capacity of the source table,
// and write capacity of the destination table, this functions returns
// read(src) and write(dst) as a struct
func (sync *syncState) getCapacity(tableName string, dynamo *dynamodb.DynamoDB) provisionedThroughput {
	var err error = nil
	var input *dynamodb.DescribeTableInput
	var output *dynamodb.DescribeTableOutput

	input = &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}

	for i := 0; i < maxRetries; i++ {
		output, err = dynamo.DescribeTable(input)
		if err != nil {
			logger.WithFields(logging.Fields{
				"Error": err,
				"Table": tableName,
			}).Error("Error in reading provisioned throughput")
		} else {
			result := output.Table.ProvisionedThroughput
			logger.WithFields(logging.Fields{
				"Table":          tableName,
				"Read Capacity":  *result.ReadCapacityUnits,
				"Write Capacity": *result.WriteCapacityUnits,
			}).Info("Fetched provisioned throughput of table")
			return provisionedThroughput{
				*result.ReadCapacityUnits,
				*result.WriteCapacityUnits,
			}
		}
	}

	return provisionedThroughput{-1, -1}
}

func (sync *syncState) getTableSize(table string, dynamo *dynamodb.DynamoDB) int64 {
	var input *dynamodb.DescribeTableInput
	var size int64 = 0
	input = &dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	}

	for i := 0; i < maxRetries; i++ {
		output, err := dynamo.DescribeTable(input)
		if err != nil {
			logger.WithFields(logging.Fields{
				"Error": err,
				"Table": table,
			}).Debug("Error in getting table size")
		} else {
			size = *output.Table.TableSizeBytes
		}
	}

	return size
}

func (sync *syncState) createTable(key primaryKey, properties *dynamodb.DescribeTableOutput) error {
	logger.WithFields(logging.Fields{
		"Destination Table": key.dstTable,
	}).Info("Creating table")

	input := &dynamodb.CreateTableInput{
		TableName:            aws.String(sync.tableConfig.DstTable),
		KeySchema:            properties.Table.KeySchema,
		AttributeDefinitions: properties.Table.AttributeDefinitions,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  properties.Table.ProvisionedThroughput.ReadCapacityUnits,
			WriteCapacityUnits: properties.Table.ProvisionedThroughput.WriteCapacityUnits,
		},
	}
	_, err := sync.dstDynamo.CreateTable(input)

	// Wait for table to be created
	status := ""
	for status != "ACTIVE" {
		output, _ := sync.dstDynamo.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(sync.tableConfig.DstTable),
		})
		status = *output.Table.TableStatus
		logger.WithFields(logging.Fields{
			"Destination Table": key.dstTable,
		}).Debug("Waiting for destination table to be created")
		time.Sleep(1 * time.Second)
	}

	return err
}

func (sync *syncState) deleteTable(key primaryKey) error {
	logger.WithFields(logging.Fields{
		"Destination Table": key.dstTable,
	}).Info("Dropping items from stale table")

	if strings.Contains(strings.ToLower(sync.tableConfig.DstTable), "prod") ||
		strings.Contains(strings.ToLower(sync.tableConfig.DstTable), "production") {
		logger.WithFields(logging.Fields{
			"Destination Table": sync.tableConfig.DstTable,
		}).Info("Warning! The table you are trying to delete might be a " +
			"production table. Double check the source and destination tables.")
		return errors.New("will not delete a table with `production` in its name")
	}

	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(sync.tableConfig.DstTable),
	}
	var err error

	for i := 0; i < maxRetries; i++ {
		_, err = sync.dstDynamo.DeleteTable(input)
		if err != nil {
			logger.WithFields(logging.Fields{
				"Destination Table": sync.tableConfig.DstTable,
			}).Debug("Failed to delete table. Retry in progress")
		} else {
			break
		}
	}

	if err != nil {
		logger.WithFields(logging.Fields{
			"Destination Table": key.dstTable,
			"Error":             err,
		}).Info("Failed to delete table")
	} else {
		logger.WithFields(logging.Fields{
			"Destination Table": key.dstTable,
		}).Info("Deleted dynamodb table")
	}

	return err
}
