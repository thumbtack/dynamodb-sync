package main

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	logging "github.com/sirupsen/logrus"
)

const (
	partitionKey = "pk"
)

var keySchema = []*dynamodb.KeySchemaElement{
	{
		AttributeName: aws.String(partitionKey),
		KeyType:       aws.String(dynamodb.KeyTypeHash),
	},
}

var attributeDefinitions = []*dynamodb.AttributeDefinition{
	{
		AttributeName: aws.String(partitionKey),
		AttributeType: aws.String("N"),
	},
}

var throughput = &dynamodb.ProvisionedThroughput{
	ReadCapacityUnits:  aws.Int64(int64(20000)),
	WriteCapacityUnits: aws.Int64(int64(10000)),
}

var checkpointThroughput = &dynamodb.ProvisionedThroughput{
	ReadCapacityUnits:  aws.Int64(int64(10)),
	WriteCapacityUnits: aws.Int64(int64(10)),
}

var checkPointKeySchema = []*dynamodb.KeySchemaElement{
	{
		AttributeName: aws.String(sourceTable),
		KeyType:       aws.String(dynamodb.KeyTypeHash),
	},
	{
		AttributeName: aws.String(dstTable),
		KeyType:       aws.String(dynamodb.KeyTypeRange),
	},
}

var cpAttributeDefinitions = []*dynamodb.AttributeDefinition{
	{
		AttributeName: aws.String(sourceTable),
		AttributeType: aws.String("S"),
	},
	{
		AttributeName: aws.String(dstTable),
		AttributeType: aws.String("S"),
	},
}

var streamSpec = &dynamodb.StreamSpecification{
	StreamEnabled:  aws.Bool(true),
	StreamViewType: aws.String("NEW_AND_OLD_IMAGES"),
}

func (app *appConfig) createCheckpointTable(checkpointDynamo *dynamodb.DynamoDB) {
	logger.WithFields(logging.Fields{
		"Table Name": os.Getenv(paramCheckpointTable),
	}).Info("Creating checkpoint table")
	_, err := checkpointDynamo.CreateTable(&dynamodb.CreateTableInput{
		TableName:             aws.String(os.Getenv(paramCheckpointTable)),
		KeySchema:             checkPointKeySchema,
		AttributeDefinitions:  cpAttributeDefinitions,
		ProvisionedThroughput: checkpointThroughput,
	})

	if err != nil {
		logger.WithFields(logging.Fields{
			"Error": err,
		}).Error("Failed to create checkpoint table")
	}

	status := ""
	for status != "ACTIVE" {
		logger.WithFields(logging.Fields{
			"Table Name": os.Getenv(paramCheckpointTable),
		}).Debug("Waiting for table to be created")
		time.Sleep(1000 * time.Millisecond)
		response, _ := checkpointDynamo.DescribeTable(
			&dynamodb.DescribeTableInput{
				TableName: aws.String(os.Getenv(paramCheckpointTable)),
			},
		)
		status = *response.Table.TableStatus
	}
}

func setupTest(ss *syncState, key primaryKey) {
	var err error

	table := [2]string{key.sourceTable, key.dstTable}
	dynamo := [2]*dynamodb.DynamoDB{ss.srcDynamo, ss.dstDynamo}
	for i, name := range table {
		logger.WithFields(logging.Fields{
			"Table Name": name,
		}).Info("Creating table")
		_, err = dynamo[i].CreateTable(&dynamodb.CreateTableInput{
			TableName:             aws.String(name),
			KeySchema:             keySchema,
			AttributeDefinitions:  attributeDefinitions,
			ProvisionedThroughput: throughput,
			StreamSpecification:   streamSpec,
		})
		if err != nil {
			logger.WithFields(logging.Fields{
				"Table Name": name,
			}).Error(err)
		}

		status := ""
		for status != "ACTIVE" {
			logger.WithFields(logging.Fields{
				"Table Name": name,
			}).Debug("Waiting for table to be created")
			time.Sleep(1000 * time.Millisecond)
			response, _ := dynamo[i].DescribeTable(
				&dynamodb.DescribeTableInput{
					TableName: aws.String(name),
				})
			status = *response.Table.TableStatus
		}
	}
}

func (ss *syncState) scanTable(name string) {
	lastEvaluatedKey := make(map[string]*dynamodb.AttributeValue, 0)
	items := make([]map[string]*dynamodb.AttributeValue, 0)
	input := &dynamodb.ScanInput{TableName: aws.String(name)}

	for {
		if len(lastEvaluatedKey) > 0 {
			input.ExclusiveStartKey = lastEvaluatedKey
		}
		for i := 1; i <= maxRetries; i++ {
			result, err := ss.srcDynamo.Scan(input)
			if err != nil {
				if i == maxRetries {
					return
				}
				backoff(i, "Scan")
			} else {
				lastEvaluatedKey = result.LastEvaluatedKey
				items = append(items, result.Items...)
				if len(lastEvaluatedKey) == 0 {
					logger.WithFields(logging.Fields{
						"Table": name,
					}).Info("Scan successful")
					return
				}
				break
			}
		}
	}
}

func (ss *syncState) teardown(key primaryKey) {
	table := [2]string{key.sourceTable, key.dstTable}
	dynamo := [2]*dynamodb.DynamoDB{ss.srcDynamo,
		ss.dstDynamo}

	for i, name := range table {
		logger.WithFields(logging.Fields{
			"Table": name,
		}).Info("Tearing down table")
		_, err := dynamo[i].DeleteTable(&dynamodb.DeleteTableInput{
			TableName: aws.String(name),
		})

		if err != nil {
			logger.WithFields(logging.Fields{
				"Table": name,
			}).Error("Can't delete table that does not exist")
			continue
		}
		status := "DELETING"
		for status == "DELETING" {
			logger.WithFields(logging.Fields{
				"Table": name,
			}).Info("Waiting for table to be deleted")
			time.Sleep(1000 * time.Millisecond)
			response, _ := dynamo[i].DescribeTable(&dynamodb.DescribeTableInput{
				TableName: aws.String(name),
			})
			if response == nil || response.Table == nil {
				break
			}
			status = *response.Table.TableStatus
		}
	}
}

func deleteCheckpointTable(checkpointDynamo *dynamodb.DynamoDB) {
	logger.WithFields(logging.Fields{"Table": os.Getenv(paramCheckpointTable)}).Info("Tearing down table")
	_, err := checkpointDynamo.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(os.Getenv(paramCheckpointTable)),
	})

	if err != nil {
		logger.WithFields(logging.Fields{
			"Table": os.Getenv(paramCheckpointTable),
			"Error": err,
		}).Error("Can't delete table")
	}
	status := "DELETING"
	for status == "DELETING" {
		time.Sleep(1000 * time.Millisecond)
		response, _ := checkpointDynamo.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(os.Getenv(paramCheckpointTable)),
		})
		if response == nil || response.Table == nil {
			break
		}
		status = *response.Table.TableStatus
	}
}

func (ss *syncState) continuousWrite(quit <-chan bool, key primaryKey) {
	i := 10

	for {
		select {
		case <-quit:
			return
		default:
			input := &dynamodb.PutItemInput{
				TableName: aws.String(key.sourceTable),
				Item: map[string]*dynamodb.AttributeValue{
					partitionKey: {N: aws.String(strconv.FormatInt(int64(i), 10))},
				},
			}
			_, err := ss.srcDynamo.PutItem(input)
			if err != nil {
				logger.WithFields(logging.Fields{
					"Error": err,
				}).Fatal("Failed to insert item")
			}
			time.Sleep(100 * time.Millisecond)
			i++
		}
	}
}

func (ss *syncState) testStreamSyncWait() {
	quit := make(chan bool)
	quit2 := make(chan bool)
	k := primaryKey{ss.tableConfig.SrcTable, ss.tableConfig.DstTable}
	go ss.continuousWrite(quit, k)
	go ss.replicate(quit2, k)
	// Allow some time to sync
	time.Sleep(5 * time.Second)
	// Stop writing
	quit <- true
	// Wait for some time
	time.Sleep(5 * time.Second)
	// Continue writing once again, does the stream ss resume?
	go ss.continuousWrite(quit, k)
	time.Sleep(5 * time.Second)
	quit <- true
	// Block on streamSync
	<-quit2
}

func (ss *syncState) testExpireShards() {
	for random := range ss.checkpoint {
		k := primaryKey{ss.tableConfig.SrcTable, ss.tableConfig.DstTable}
		ss.expireCheckpointLocal(k, aws.String(random))
		ss.expireCheckpointRemote(k, random)
		break
	}
}

func TestAll(t *testing.T) {
	print("Starting test")
	//var err error
	_ = os.Setenv(paramConfigDir, "local")
	_ = os.Setenv(paramVerbose, "1")
	_ = os.Setenv(paramCheckpointRegion, "us-west-2")
	_ = os.Setenv(paramCheckpointTable, "local-dynamodb-ss.checkpoint")
	_ = os.Setenv(paramCheckpointEndpoint, "http://localhost:8000")

	app := NewApp()
	checkpointDynamo := dynamodb.New(session.Must(
		session.NewSession(
			aws.NewConfig().
				WithRegion(os.Getenv(paramCheckpointRegion)).
				WithEndpoint(os.Getenv(paramCheckpointEndpoint)).
				WithMaxRetries(maxRetries),
		)))

	deleteCheckpointTable(checkpointDynamo)
	app.createCheckpointTable(checkpointDynamo)

	var syncWorkers = make([]*syncState, len(app.sync))

	print(len(app.sync))

	for i := 0; i < len(app.sync); i++ {
		syncWorkers[i] = NewSyncState(app.sync[i])
	}

	for i := 0; i < len(syncWorkers); i++ {
		syncWorkers[i].teardown(primaryKey{app.sync[i].SrcTable, app.sync[i].DstTable})
	}

	for i := 0; i < len(syncWorkers); i++ {
		setupTest(syncWorkers[i], primaryKey{app.sync[i].SrcTable, app.sync[i].DstTable})
	}

	//main()

	for i := 0; i < len(syncWorkers); i++ {
		syncWorkers[i].testStreamSyncWait()
		syncWorkers[i].testExpireShards()
	}
}
