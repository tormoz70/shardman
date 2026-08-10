package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	GRPCAddr            string
	HTTPAddr            string
	MetadataDSN         string
	ClusterKey          string
	SealCheckInterval   time.Duration
	DrainTimeout        time.Duration
	HeartbeatTimeout    time.Duration
	HealthCheckInterval time.Duration
	MetadataMaxConns    int32
}

func LoadServer() (Config, error) {
	c := Config{
		GRPCAddr:            getenv("GRPC_ADDR", ":9090"),
		HTTPAddr:            getenv("HTTP_ADDR", ":8080"),
		MetadataDSN:         os.Getenv("METADATA_PG_DSN"),
		ClusterKey:          os.Getenv("CLUSTER_KEY"),
		SealCheckInterval:   durationEnv("SEAL_CHECK_INTERVAL", 30*time.Second),
		DrainTimeout:        durationEnv("DRAIN_TIMEOUT", 30*time.Second),
		HeartbeatTimeout:    durationEnv("HEARTBEAT_TIMEOUT", 60*time.Second),
		HealthCheckInterval: durationEnv("HEALTH_CHECK_INTERVAL", 15*time.Second),
		MetadataMaxConns:    int32(intEnv("METADATA_PG_MAX_CONNS", 20)),
	}
	if c.MetadataDSN == "" {
		return c, fmt.Errorf("METADATA_PG_DSN required")
	}
	return c, nil
}

type AgentConfig struct {
	PGDSN          string
	ShardUUID      string
	CoordinatorAddr string
	ClusterKey     string
	StatsInterval  time.Duration
	SizeSource     string
	AppDBRole      string
	DrainMode      string
}

func LoadAgent() (AgentConfig, error) {
	c := AgentConfig{
		PGDSN:           os.Getenv("PG_DSN"),
		ShardUUID:       os.Getenv("SHARD_UUID"),
		CoordinatorAddr: getenv("COORDINATOR_ADDR", os.Getenv("COORDINATOR_URL")),
		ClusterKey:      os.Getenv("CLUSTER_KEY"),
		StatsInterval:   durationEnv("STATS_INTERVAL", 15*time.Second),
		SizeSource:      getenv("SIZE_SOURCE", "database"),
		AppDBRole:       os.Getenv("APP_DB_ROLE"),
		DrainMode:       getenv("DRAIN_MODE", "revoke"),
	}
	if c.PGDSN == "" || c.ShardUUID == "" || c.CoordinatorAddr == "" {
		return c, fmt.Errorf("PG_DSN, SHARD_UUID, COORDINATOR_ADDR required")
	}
	if c.SizeSource != "database" && c.SizeSource != "relations" {
		return c, fmt.Errorf("SIZE_SOURCE must be database or relations")
	}
	return c, nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func durationEnv(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func intEnv(k string, def int64) int64 {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}
