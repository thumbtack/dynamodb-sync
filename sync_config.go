package main

import (
	"errors"
	"strings"
)

// syncConfig is read from the config file for each sync job.
type syncConfig struct {
	SrcTable                  string `json:"src_table"`
	DstTable                  string `json:"dst_table"`
	SrcRegion                 string `json:"src_region"`
	DstRegion                 string `json:"dst_region"`
	SrcEndpoint               string `json:"src_endpoint"`
	DstEndpoint               string `json:"dst_endpoint"`
	SrcEnv                    string `json:"src_env"`
	DstEnv                    string `json:"dst_env"`
	ReadWorkers               int    `json:"read_workers"`
	WriteWorkers              int    `json:"write_workers"`
	ReadQPS                   int64  `json:"read_qps"`
	WriteQPS                  int64  `json:"write_qps"`
	UpdateCheckpointThreshold int    `json:"update_checkpoint_threshold"`
	EnableStreaming           *bool  `json:"enable_streaming"`
}

func (sc *syncConfig) setDefault() error {
	if sc.SrcTable == "" || sc.DstTable == "" || sc.SrcRegion == "" ||
		sc.DstRegion == "" || sc.SrcEnv == "" || sc.DstEnv == "" {
		return errors.New("table and region for source and destination are mandatory")
	}
	if sc.ReadQPS == 0 {
		sc.ReadQPS = 400
	}
	if sc.WriteQPS == 0 {
		sc.WriteQPS = 400
	}
	if sc.ReadWorkers == 0 {
		sc.ReadWorkers = 4
	}
	if sc.WriteWorkers == 0 {
		sc.WriteWorkers = 16
	}
	if sc.UpdateCheckpointThreshold == 0 {
		sc.UpdateCheckpointThreshold = 25
	}
	if sc.EnableStreaming == nil {
		val := true
		sc.EnableStreaming = &val
	}
	return nil
}

func (sc *syncConfig) getCheckpointPK() primaryKey {
	key := primaryKey{
		sourceTable: sc.SrcTable,
		dstTable:    sc.DstTable,
	}
	if strings.Contains(sc.SrcEnv, "new") {
		key.sourceTable += ".account." + strings.Split(sc.SrcEnv, "_")[0]
	}
	if strings.Contains(sc.DstEnv, "new") {
		key.dstTable += ".account." + strings.Split(sc.DstEnv, "_")[0]
	}
	return key
}
