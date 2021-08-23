package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func Test_generateGsiUpdate(t *testing.T) {
	tests := []struct {
		name             string
		originalCapacity Throughput
		deltaCapacity    *provisionedThroughput
		expect           []*dynamodb.GlobalSecondaryIndexUpdate
	}{
		{
			name: "no gsi",
			originalCapacity: Throughput{
				table: provisionedThroughput{1, 2},
			},
			deltaCapacity: &provisionedThroughput{1, 2},
			expect:        []*dynamodb.GlobalSecondaryIndexUpdate(nil),
		},
		{
			name: "one gsi",
			originalCapacity: Throughput{
				table: provisionedThroughput{1, 1},
				gsi: map[string]provisionedThroughput{
					"first-gsi": provisionedThroughput{10, 10},
				},
			},
			deltaCapacity: &provisionedThroughput{5, 5},
			expect: []*dynamodb.GlobalSecondaryIndexUpdate{
				{
					Update: &dynamodb.UpdateGlobalSecondaryIndexAction{
						IndexName: aws.String("first-gsi"),
						ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
							ReadCapacityUnits:  aws.Int64(15),
							WriteCapacityUnits: aws.Int64(15),
						},
					},
				},
			},
		},
		{
			name: "two gsi",
			originalCapacity: Throughput{
				table: provisionedThroughput{1, 1},
				gsi: map[string]provisionedThroughput{
					"first-gsi":  {10, 10},
					"second-gsi": {20, 20},
				},
			},
			deltaCapacity: &provisionedThroughput{5, 5},
			expect: []*dynamodb.GlobalSecondaryIndexUpdate{
				{
					Update: &dynamodb.UpdateGlobalSecondaryIndexAction{
						IndexName: aws.String("first-gsi"),
						ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
							ReadCapacityUnits:  aws.Int64(15),
							WriteCapacityUnits: aws.Int64(15),
						},
					},
				},
				{
					Update: &dynamodb.UpdateGlobalSecondaryIndexAction{
						IndexName: aws.String("second-gsi"),
						ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
							ReadCapacityUnits:  aws.Int64(25),
							WriteCapacityUnits: aws.Int64(25),
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		output := generateGsiUpdate(test.originalCapacity, test.deltaCapacity)
		assert.Equal(t, test.expect, output)
	}
}
