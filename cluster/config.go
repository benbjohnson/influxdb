package cluster

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	// DefaultWriteTimeout is the default timeout for a complete write to succeed.
	DefaultWriteTimeout = 5 * time.Second

	// DefaultShardWriterTimeout is the default timeout set on shard writers.
	DefaultShardWriterTimeout = 5 * time.Second

	// DefaultShardQueryTimeout is the default timeout set for query operations.
	DefaultShardQueryTimeout = 5 * time.Second
)

// Config represents the configuration for the clustering service.
type Config struct {
	ForceRemoteShardMapping bool          `toml:"force-remote-mapping"`
	WriteTimeout            toml.Duration `toml:"write-timeout"`
	ShardWriterTimeout      toml.Duration `toml:"shard-writer-timeout"`
	ShardQueryTimeout       toml.Duration `toml:"shard-query-timeout"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		WriteTimeout:       toml.Duration(DefaultWriteTimeout),
		ShardWriterTimeout: toml.Duration(DefaultShardWriterTimeout),
		ShardQueryTimeout:  toml.Duration(DefaultShardQueryTimeout),
	}
}
