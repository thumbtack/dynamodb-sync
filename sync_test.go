package main

import (
	"os"
	"strconv"
	"time"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws"
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

var provisionedThroughput = &dynamodb.ProvisionedThroughput{
	ReadCapacityUnits:  aws.Int64(int64(100)),
	WriteCapacityUnits: aws.Int64(int64(100)),
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

var streamSpec = &dynamodb.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: aws.String("NEW_AND_OLD_IMAGES")}

func (app *appConfig) createCheckpointTable() {
	app.logger.WithFields(logging.Fields{
		"Table Name": app.ddbTable,
	}).Info("Creating checkpoint table")
	_, err := app.ddbClient.CreateTable(&dynamodb.CreateTableInput{
		TableName:             aws.String(app.ddbTable),
		KeySchema:             checkPointKeySchema,
		AttributeDefinitions:  cpAttributeDefinitions,
		ProvisionedThroughput: provisionedThroughput,
	})

	if err != nil {
		app.logger.WithFields(logging.Fields{
			"Error": err,
		}).Error("Failed to create checkpoint table")
	}

	status := ""
	for status != "ACTIVE" {
		app.logger.WithFields(logging.Fields{
			"Table Name": app.ddbTable,
		}).Debug("Waiting for table to be created")
		time.Sleep(1000 * time.Millisecond)
		response, _ := app.ddbClient.DescribeTable(
			&dynamodb.DescribeTableInput{
				TableName: aws.String(app.ddbTable),
			},
		)
		status = *response.Table.TableStatus
	}
}

func setupTest(app *appConfig, key primaryKey) {
	var err error

	table := [2]string{key.sourceTable, key.dstTable}
	dynamo := [2]*dynamodb.DynamoDB{app.sync[key].srcDynamo, app.sync[key].dstDynamo}
	for i, name := range table {
		app.logger.WithFields(logging.Fields{
			"Table Name": name,
		}).Info("Creating table")
		_, err = dynamo[i].CreateTable(&dynamodb.CreateTableInput{
			TableName:             aws.String(name),
			KeySchema:             keySchema,
			AttributeDefinitions:  attributeDefinitions,
			ProvisionedThroughput: provisionedThroughput,
			StreamSpecification:   streamSpec,
		})
		if err != nil {
			app.logger.WithFields(logging.Fields{
				"Table Name": name,
			}).Error("Table already exists")
		}

		status := ""
		for status != "ACTIVE" {
			app.logger.WithFields(logging.Fields{
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

func (app *appConfig) scanTable(key primaryKey, name string) {
	lastEvaluatedKey := make(map[string]*dynamodb.AttributeValue, 0)
	maxConnectRetries := app.sync[key].MaxConnectRetries
	items := make([]map[string]*dynamodb.AttributeValue, 0)
	input := &dynamodb.ScanInput{TableName: aws.String(name)}

	for {
		if len(lastEvaluatedKey) > 0 {
			input.ExclusiveStartKey = lastEvaluatedKey
		}
		for i := 0; i < maxConnectRetries; i++ {
			result, err := app.sync[key].srcDynamo.Scan(input)
			if err != nil {
				if i == maxConnectRetries-1 {
					return
				}
				app.backoff(i, "Scan")
			} else {
				lastEvaluatedKey = result.LastEvaluatedKey
				items = append(items, result.Items...)
				if len(lastEvaluatedKey) == 0 {
					app.logger.WithFields(logging.Fields{
						"Table": name,
					}).Info("Scan successful")
					return
				}
				break
			}
		}
	}
}

func (app *appConfig) teardown(key primaryKey) {
	table := [2]string{key.sourceTable, key.dstTable}
	dynamo := [2]*dynamodb.DynamoDB{app.sync[key].srcDynamo,
		app.sync[key].dstDynamo}

	for i, name := range table {
		app.logger.WithFields(logging.Fields{
			"Table": name,
		}).Info("Tearing down table")
		_, err := dynamo[i].DeleteTable(&dynamodb.DeleteTableInput{
			TableName: aws.String(name),
		})

		if err != nil {
			app.logger.WithFields(logging.Fields{
				"Table": name,
			}).Error("Can't delete table that does not exist")
			continue
		}
		status := "DELETING"
		for status == "DELETING" {
			app.logger.WithFields(logging.Fields{
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

func (app *appConfig) continuousWrite(quit <-chan bool, key primaryKey) {
	i := 10

	for {
		select {
		case <-quit:
			return
		default:
			input := &dynamodb.PutItemInput{
				TableName: aws.String(app.sync[key].SrcTable),
				Item: map[string]*dynamodb.AttributeValue{
					partitionKey: {N: aws.String(strconv.FormatInt(int64(i), 10))},
				},
			}
			_, err := app.sync[key].srcDynamo.PutItem(input)
			if err != nil {
				app.logger.WithFields(logging.Fields{
					"Error": err,
				}).Fatal("Failed to insert item")
			}
			time.Sleep(100 * time.Millisecond)
			i++
		}
	}
}

func (app *appConfig) testStreamSyncWait() {
	for k, _ := range app.sync {
		quit := make(chan bool)
		quit2 := make(chan bool)
		go app.continuousWrite(quit, k)
		go app.replicate(quit2, k)
		// Allow some time to sync
		time.Sleep(5 * time.Second)
		// Stop writing
		quit <- true
		// Wait for some time
		time.Sleep(5 * time.Second)
		// Continue writing once again, does the stream sync resume?
		go app.continuousWrite(quit, k)
		time.Sleep(5 * time.Second)
		quit <- true
		// Block on streamSync
		<-quit2
	}
}

func (app *appConfig) testExpireShards() {
	for k, v := range app.state {
		for random, _ := range v.checkpoint {
			app.expireCheckpointLocal(k, aws.String(random))
			app.expireCheckpointRemote(k, random)
			break
		}
	}
}

func TestAll(t *testing.T) {
	print("Starting test")
	//var err error
	os.Setenv(paramConfigDir, "local")
	os.Setenv(paramVerbose, "1")
	os.Setenv(paramCheckpointRegion, "us-west-2")
	os.Setenv(paramCheckpointTable, "local-dynamodb-sync.checkpoint")
	os.Setenv(paramCheckpointEndpoint, "http://localhost:8000")
	os.Setenv(paramMaxRetries, "3")

	app := NewApp()
	app.createCheckpointTable()
	app.connect()

	for k, _ := range app.sync {
		app.teardown(k)
	}

	for k, _ := range app.sync {
		setupTest(app, k)
	}

	app.loadCheckpointTable()
	app.addNewStateTracker()

	app.testStreamSyncWait()
	app.testExpireShards()
	/*for k, _ := range app.sync {
		app.scanTable(k, k.sourceTable)
		app.scanTable(k, k.dstTable)
	}*/
}
