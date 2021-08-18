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

// provisionedThroughput is the basic throughput struct
type provisionedThroughput struct {
	readCapacity  int64
	writeCapacity int64
}

// Throughput describes the table throughput, include both the table and its gsi
type Throughput struct {
	table provisionedThroughput
	gsi   map[string]provisionedThroughput
}

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

func increaseCapacity(
	tableName string,
	dynamo *dynamodb.DynamoDB,
	originalCapacity Throughput,
	deltaCapacity provisionedThroughput,
) error {
	logger.WithFields(logging.Fields{
		"table":            tableName,
		"originalCapacity": originalCapacity,
		"added RCU":        deltaCapacity.readCapacity,
		"added WCU":        deltaCapacity.writeCapacity,
	}).Info("increasing capacity")
	_, err := dynamo.UpdateTable(&dynamodb.UpdateTableInput{
		TableName: aws.String(tableName),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits: aws.Int64(
				originalCapacity.table.readCapacity + deltaCapacity.readCapacity),
			WriteCapacityUnits: aws.Int64(
				originalCapacity.table.writeCapacity + deltaCapacity.writeCapacity),
		},
		GlobalSecondaryIndexUpdates: generateGsiUpdate(originalCapacity, &deltaCapacity),
	})
	if err != nil {
		logger.WithFields(logging.Fields{
			"table": tableName,
			"error": err,
		}).Error("failed to increase capacity")
		return err
	}
	waitForTableUpdate(tableName, dynamo)
	return nil
}

func decreaseCapacity(
	tableName string,
	dynamo *dynamodb.DynamoDB,
	originalCapacity Throughput,
) error {
	logger.WithFields(logging.Fields{
		"table":            tableName,
		"originalCapacity": originalCapacity,
	}).Info("decreasing capacity")
	_, err := dynamo.UpdateTable(&dynamodb.UpdateTableInput{
		TableName: aws.String(tableName),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(originalCapacity.table.readCapacity),
			WriteCapacityUnits: aws.Int64(originalCapacity.table.writeCapacity),
		},
		GlobalSecondaryIndexUpdates: generateGsiUpdate(originalCapacity, nil),
	})
	if err != nil {
		logger.WithFields(logging.Fields{
			"table": tableName,
			"error": err,
		}).Error("failed to decrease capacity")
		return err
	}
	waitForTableUpdate(tableName, dynamo)
	return nil
}

func generateGsiUpdate(
	originalCapacity Throughput,
	deltaCapacity *provisionedThroughput,
) []*dynamodb.GlobalSecondaryIndexUpdate {
	var result []*dynamodb.GlobalSecondaryIndexUpdate
	for indexName, capacity := range originalCapacity.gsi {
		readCapacity, writeCapacity := capacity.readCapacity, capacity.writeCapacity
		if deltaCapacity != nil {
			readCapacity += deltaCapacity.readCapacity
			writeCapacity += deltaCapacity.writeCapacity
		}
		result = append(result, &dynamodb.GlobalSecondaryIndexUpdate{
			Update: &dynamodb.UpdateGlobalSecondaryIndexAction{
				IndexName: aws.String(indexName),
				ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(readCapacity),
					WriteCapacityUnits: aws.Int64(writeCapacity),
				},
			},
		})
	}
	return result
}

func waitForTableUpdate(tableName string, dynamo *dynamodb.DynamoDB) {
	status := ""
	statusInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	for status != "ACTIVE" {
		output, err := dynamo.DescribeTable(statusInput)
		if err != nil {
			logger.WithFields(logging.Fields{
				"table": tableName,
				"error":     err,
			}).Error("failed to get the table status")
			// likely an internal error from DDB, nothing can be done here
			break
		} else {
			status = *output.Table.TableStatus
			logger.WithFields(logging.Fields{
				"table": tableName,
			}).Debug("Updating table throughput")
			time.Sleep(1 * time.Second)
		}
	}
	if status == "ACTIVE" {
		logger.WithFields(logging.Fields{
			"Table": tableName,
		}).Info("Successfully updated table throughput")
	}
}

// getCapacity returns the read and write capacity of the given table and its gsi
func getCapacity(tableName string, dynamo *dynamodb.DynamoDB) (*Throughput, error) {
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
	throughput := Throughput{
		table: provisionedThroughput{
			readCapacity:  *output.Table.ProvisionedThroughput.ReadCapacityUnits,
			writeCapacity: *output.Table.ProvisionedThroughput.WriteCapacityUnits,
		},
	}
	if len(output.Table.GlobalSecondaryIndexes) > 0 {
		throughput.gsi = map[string]provisionedThroughput{}
		for _, gsi := range output.Table.GlobalSecondaryIndexes {
			throughput.gsi[*gsi.IndexName] = provisionedThroughput{
				readCapacity:  *gsi.ProvisionedThroughput.ReadCapacityUnits,
				writeCapacity: *gsi.ProvisionedThroughput.WriteCapacityUnits,
			}
		}
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
