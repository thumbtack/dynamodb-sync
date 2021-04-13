package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetDefault_missing_DstEnv(t *testing.T) {
	mockConfig := MockSyncConfig()
	mockConfig.DstEnv = ""
	err := mockConfig.setDefault()
	assert.Error(t, err, "setDefault should have thrown err")
}

func TestSetDefault(t *testing.T) {
	falseVal, trueVal := false, true

	tests := []struct {
		name string

		readQPS     int64
		writeQPS    int64
		readWorker  int
		writeWorker int
		threshold   int
		stream      *bool

		expectReadQPS     int64
		expectWriteQPS    int64
		expectReadWorker  int
		expectWriteWorker int
		expectThreshold   int
		expectStream      *bool
	}{
		{
			name: "bare minimum setup",

			expectReadQPS:     400,
			expectWriteQPS:    400,
			expectReadWorker:  4,
			expectWriteWorker: 16,
			expectThreshold:   25,
			expectStream:      &trueVal,
		},
		{
			name: "choose random name to set",

			readQPS:    200,
			readWorker: 2,
			threshold:  50,
			stream:     &falseVal,

			expectReadQPS:     200,
			expectWriteQPS:    400,
			expectReadWorker:  2,
			expectWriteWorker: 16,
			expectThreshold:   50,
			expectStream:      &falseVal,
		},
	}

	for _, test := range tests {
		mockConfig := MockSyncConfig()
		mockConfig.ReadQPS = test.readQPS
		mockConfig.WriteQPS = test.writeQPS
		mockConfig.ReadWorkers = test.readWorker
		mockConfig.WriteWorkers = test.writeWorker
		mockConfig.UpdateCheckpointThreshold = test.threshold
		mockConfig.EnableStreaming = test.stream

		err := mockConfig.setDefault()
		assert.Nilf(t, err, "%s failed", test.name)
		assert.Equalf(
			t, test.expectReadQPS, mockConfig.ReadQPS,
			"%s ReadQPS failed", test.name)
		assert.Equalf(
			t, test.expectWriteQPS, mockConfig.WriteQPS,
			"%s WriteQPS failed", test.name)
		assert.Equalf(
			t, test.expectReadWorker, mockConfig.ReadWorkers,
			"%s ReadWorkers failed", test.name)
		assert.Equalf(
			t, test.expectWriteWorker, mockConfig.WriteWorkers,
			"%s WriteWorkers failed", test.name)
		assert.Equalf(
			t, test.expectThreshold, mockConfig.UpdateCheckpointThreshold,
			"%s UpdateCheckpointThreshold failed", test.name)
		assert.Equalf(
			t, test.expectStream, mockConfig.EnableStreaming,
			"%s EnableStreaming failed", test.name)
	}
}

func TestGetCheckpointPK(t *testing.T) {
	tests := []struct{
		name string
		srcEnv string
		dstEnv string
		expectCheckpoint primaryKey
	} {
		{
			name: "bare minimum setup",
			srcEnv: "production",
			dstEnv: "staging",
			expectCheckpoint: primaryKey{
				sourceTable: "src-table",
				dstTable: "dst-table",
			},
		},
		{
			name: "env contains `new`",
			srcEnv: "production_new",
			dstEnv: "staging_new",
			expectCheckpoint: primaryKey{
				sourceTable: "src-table.account.production",
				dstTable: "dst-table.account.staging",
			},
		},
	}

	for _, test := range tests {
		mockConfig := MockSyncConfig()
		mockConfig.SrcEnv = test.srcEnv
		mockConfig.DstEnv = test.dstEnv

		pk := mockConfig.getCheckpointPK()
		assert.Equalf(t, test.expectCheckpoint, pk, "%s failed", test.name)
	}
}