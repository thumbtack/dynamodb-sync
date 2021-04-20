package main

import (
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

// syncState represents each state during the sync
type syncState struct {
	tableConfig           *syncConfig
	srcDynamo             *dynamodb.DynamoDB
	dstDynamo             *dynamodb.DynamoDB
	stream                *dynamodbstreams.DynamoDBStreams
	completedShardLock    sync.RWMutex
	activeShardLock       sync.RWMutex
	checkpointLock        sync.RWMutex
	activeShardProcessors map[string]bool
	expiredShards         map[string]bool
	checkpoint            map[string]string
	checkpointPK          primaryKey
	recordCounter         int
	timestamp             time.Time
}

// NewSyncState sets the sync state based on the config
func NewSyncState(tableConfig *syncConfig, checkpointPK primaryKey) *syncState {
	httpClient := &http.Client{
		Timeout: 8 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:    2048,
			MaxConnsPerHost: 1024,
		},
	}
	srcSess := getSession(tableConfig.SrcRegion, tableConfig.SrcEndpoint, httpClient)
	dstSess := getSession(tableConfig.DstRegion, tableConfig.DstEndpoint, nil)
	srcRoleArn := getRoleArn(tableConfig.SrcEnv)
	dstRoleArn := getRoleArn(tableConfig.DstEnv)

	srcCreds := stscreds.NewCredentials(srcSess, srcRoleArn)
	dstCreds := stscreds.NewCredentials(dstSess, dstRoleArn)
	srcDynamo := dynamodb.New(srcSess, &aws.Config{Credentials: srcCreds})
	dstDynamo := dynamodb.New(dstSess, &aws.Config{Credentials: dstCreds})
	stream := dynamodbstreams.New(srcSess, &aws.Config{Credentials: srcCreds})

	return &syncState{
		tableConfig:           tableConfig,
		srcDynamo:             srcDynamo,
		dstDynamo:             dstDynamo,
		stream:                stream,
		completedShardLock:    sync.RWMutex{},
		activeShardLock:       sync.RWMutex{},
		checkpointLock:        sync.RWMutex{},
		activeShardProcessors: map[string]bool{},
		expiredShards:         map[string]bool{},
		checkpoint:            map[string]string{},
		checkpointPK:          checkpointPK,
		recordCounter:         0,
		timestamp:             time.Time{},
	}
}
