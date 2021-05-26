package main

import (
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	logging "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

// Maximum size of a batch can be 25 items
// Batch Writes item to dst table
func (ss *syncState) writeBatch(
	batch map[string][]*dynamodb.WriteRequest,
	rl *rate.Limiter,
	reqCapacity float64,
	writeBatchSize int64,
) []*dynamodb.ConsumedCapacity {
	r := rl.ReserveN(time.Now(), int(reqCapacity))
	if !r.OK() {
		r = rl.ReserveN(time.Now(), int(writeBatchSize))
	}
	time.Sleep(r.Delay())

	var consumedCapacity []*dynamodb.ConsumedCapacity
	i := 0
	for len(batch) > 0 {
		output, _ := ss.dstDynamo.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: batch,
		})
		consumedCapacity = append(consumedCapacity, output.ConsumedCapacity...)

		if output.UnprocessedItems != nil {
			logger.WithFields(logging.Fields{
				"Unprocessed Items Size": len(output.UnprocessedItems),
				"Source Table":           ss.checkpointPK.sourceTable,
				"Destination Table":      ss.checkpointPK.dstTable,
			}).Debug("Some items failed to be processed")
			// exponential backoff before retrying
			backoff(i)
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
func (ss *syncState) writeTable(
	itemsChan chan []map[string]*dynamodb.AttributeValue,
	writerWG *sync.WaitGroup,
	id int,
	rl *rate.Limiter,
) {
	defer writerWG.Done()

	writeBatchSize, reqCapacity := ss.tableConfig.WriteQPS, 0.
	writeRequest := map[string][]*dynamodb.WriteRequest{}
	dst := ss.tableConfig.DstTable

	if maxBatchSize < writeBatchSize {
		writeBatchSize = maxBatchSize
	}

	for {
		items, more := <-itemsChan
		if !more {
			logger.WithFields(logging.Fields{
				"Write Worker":      id,
				"Source Table":      ss.checkpointPK.sourceTable,
				"Destination Table": ss.checkpointPK.dstTable,
			}).Debug("Write worker has finished")
			return
		}

		for _, item := range items {
			requestSize := len(writeRequest[dst])
			if int64(requestSize) == writeBatchSize {
				consumedCapacity := ss.writeBatch(writeRequest, rl, reqCapacity, writeBatchSize)
				reqCapacity = 0
				for _, each := range consumedCapacity {
					reqCapacity += *each.CapacityUnits
				}
				writeRequest[dst] = []*dynamodb.WriteRequest{}
			}
			writeRequest[dst] = append(
				writeRequest[dst],
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: item,
					},
				},
			)
		}
		// Maybe more items are left because len(items) % maxBatchSize != 0
		requestSize := len(writeRequest[dst])
		if requestSize > 0 {
			ss.writeBatch(writeRequest, rl, reqCapacity, writeBatchSize)
			writeRequest = make(map[string][]*dynamodb.WriteRequest, 0)
		}
	}
}

// readTable scans the table, and put the items into the `items` channel.
// It is not necessary that the entire table is scanned in a single call to `scan`,
// so we scan the table in a loop until the len(lastEvaluatedKey) is zero.
func (ss *syncState) readTable(
	items chan []map[string]*dynamodb.AttributeValue,
	readerWG *sync.WaitGroup,
	id int,
) {
	defer readerWG.Done()

	lastEvaluatedKey := map[string]*dynamodb.AttributeValue{}
	for {
		input := &dynamodb.ScanInput{
			TableName:      aws.String(ss.tableConfig.SrcTable),
			ConsistentRead: aws.Bool(true),
			Segment:        aws.Int64(int64(id)),
			TotalSegments:  aws.Int64(int64(ss.tableConfig.ReadWorkers)),
		}
		if len(lastEvaluatedKey) > 0 {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		successfulScan := false
		for i := 0; i < maxRetries; i++ {
			result, err := ss.srcDynamo.Scan(input)
			if err != nil {
				logger.WithFields(logging.Fields{
					"error":     err,
					"src table": ss.checkpointPK.sourceTable,
				}).Warn("Scan returned error")
				backoff(i)
			} else {
				successfulScan = true
				lastEvaluatedKey = result.LastEvaluatedKey
				items <- result.Items
				logger.WithFields(logging.Fields{
					"Scanned items size": len(result.Items),
					"Scanned Count":      *result.ScannedCount,
					"LastEvaluatedKey":   lastEvaluatedKey,
					"src table":          ss.checkpointPK.sourceTable,
				}).Debug("Scan successful")
				break
			}
		}

		if successfulScan {
			if len(lastEvaluatedKey) == 0 {
				logger.WithFields(logging.Fields{
					"src table": ss.checkpointPK.sourceTable,
				}).Debug("Scan completed")
				return
			}
		} else {
			logger.WithFields(logging.Fields{
				"src table": ss.checkpointPK.sourceTable,
			}).Error("Scan failed")
			os.Exit(1)
		}
	}
}

func updateCapacity(
	tableName string,
	newThroughput provisionedThroughput,
	dynamo *dynamodb.DynamoDB,
) error {
	logger.WithFields(logging.Fields{
		"table":   tableName,
		"new RCU": newThroughput.readCapacity,
		"new WCU": newThroughput.writeCapacity,
	}).Info("updating capacity")
	_, err := dynamo.UpdateTable(&dynamodb.UpdateTableInput{
		TableName: aws.String(tableName),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(newThroughput.readCapacity),
			WriteCapacityUnits: aws.Int64(newThroughput.writeCapacity),
		},
	})
	if err != nil {
		logger.WithFields(logging.Fields{
			"table": tableName,
			"error": err,
		}).Error("fail to update table capacity")
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
				"error": err,
				"table": tableName,
			}).Error("Failed to get table status")
			break
		} else {
			status = *output.Table.TableStatus
			logger.WithFields(logging.Fields{
				"table": tableName,
			}).Debug("Updating table throughput")
			time.Sleep(1 * time.Second)
		}
	}

	logger.WithFields(logging.Fields{
		"Table": tableName,
	}).Info("Successfully updated table throughput")

	return nil
}

// getCapacity returns the read and write capacity of the given table
func getCapacity(tableName string, dynamo *dynamodb.DynamoDB) (*provisionedThroughput, error) {
	output, err := dynamo.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		logger.WithFields(logging.Fields{
			"table": tableName,
			"error": err,
		}).Error("failed to fetch provisioned throughput")
		return nil, err
	}
	throughput := provisionedThroughput{
		readCapacity:  *output.Table.ProvisionedThroughput.ReadCapacityUnits,
		writeCapacity: *output.Table.ProvisionedThroughput.WriteCapacityUnits,
	}
	logger.WithFields(logging.Fields{
		"table":      tableName,
		"throughput": throughput,
	}).Info("fetched provisioned throughput of table")
	return &throughput, nil
}

// getTableSize returns the size, in bytes, of the given table
func getTableSize(table string, dynamo *dynamodb.DynamoDB) (*int64, error) {
	output, err := dynamo.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	})
	if err != nil {
		logger.WithFields(logging.Fields{
			"error": err,
			"table": table,
		}).Error("failed to get the table size")
		return nil, err
	}
	return output.Table.TableSizeBytes, nil
}
