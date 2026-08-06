package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr          string
	MetadataDSN       string
	ClusterKey        string
	SealCheckInterval time.Duration
	AgentReachTTL     time.Duration
}

func LoadServer() (Config, error) {
	c := Config{
		HTTPAddr:          getenv("HTTP_ADDR", ":8080"),
		MetadataDSN:       os.Getenv("METADATA_PG_DSN"),
		ClusterKey:        os.Getenv("CLUSTER_KEY"),
		SealCheckInterval: durationEnv("SEAL_CHECK_INTERVAL", 30*time.Second),
		AgentReachTTL:     durationEnv("AGENT_REACH_TTL", 2*time.Minute),
	}
	if c.MetadataDSN == "" {
		return c, fmt.Errorf("METADATA_PG_DSN required")
	}
	return c, nil
}

type AgentConfig struct {
	PGDSN          string
	ShardUUID      string
	CoordinatorURL string
	ClusterKey     string
	StatsInterval  time.Duration
}

func LoadAgent() (AgentConfig, error) {
	c := AgentConfig{
		PGDSN:          os.Getenv("PG_DSN"),
		ShardUUID:      os.Getenv("SHARD_UUID"),
		CoordinatorURL: os.Getenv("COORDINATOR_URL"),
		ClusterKey:     os.Getenv("CLUSTER_KEY"),
		StatsInterval:  durationEnv("STATS_INTERVAL", 15*time.Second),
	}
	if c.PGDSN == "" || c.ShardUUID == "" || c.CoordinatorURL == "" {
		return c, fmt.Errorf("PG_DSN, SHARD_UUID, COORDINATOR_URL required")
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

func int64Env(k string, def int64) int64 {
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
