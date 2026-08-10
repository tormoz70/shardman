//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/tormoz70/shardman/internal/bucket"
	"github.com/tormoz70/shardman/internal/fsm"
	"github.com/tormoz70/shardman/internal/resolve"
	"github.com/tormoz70/shardman/internal/retention"
	"github.com/tormoz70/shardman/internal/store"
)

const clusterKey = "integration-test-key"

// Env holds a reset metadata store and services for one test suite.
type Env struct {
	Ctx        context.Context
	Store      *store.Store
	Resolver   *resolve.Service
	Retention  *retention.Supervisor
	FixedNow   time.Time
	clusterKey string
}

func OpenEnv(t *testing.T) *Env {
	dsn := os.Getenv("METADATA_PG_DSN")
	if dsn == "" {
		t.Skip("METADATA_PG_DSN not set")
	}
	ctx := context.Background()
	st, err := store.New(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := st.ResetSchema(ctx); err != nil {
		st.Close()
		t.Fatalf("reset schema: %v", err)
	}
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	resolver := &resolve.Service{
		Store: st,
		Now:   func() time.Time { return now },
	}
	ret := &retention.Supervisor{
		Store: st,
		Now:   func() time.Time { return now },
		Log:   slog.Default(),
	}
	return &Env{
		Ctx:        ctx,
		Store:      st,
		Resolver:   resolver,
		Retention:  ret,
		FixedNow:   now,
		clusterKey: clusterKey,
	}
}

func (e *Env) Close() {
	e.Store.Close()
}

func (e *Env) SetNow(t time.Time) {
	e.FixedNow = t
	e.Resolver.Now = func() time.Time { return t }
	e.Retention.Now = func() time.Time { return t }
}

func (e *Env) MustBootstrap(cfg store.ClusterConfig) {
	if err := e.Store.Bootstrap(e.Ctx, cfg); err != nil {
		panic(fmt.Sprintf("bootstrap: %v", err))
	}
}

func (e *Env) BootstrapTime(retention, maxFuture int, maxBytes int64) {
	spec, _ := json.Marshal(map[string]string{"unit": "month"})
	rd, mf := retention, maxFuture
	e.MustBootstrap(store.ClusterConfig{
		Mode:             bucket.ModeRange,
		BucketAxis:       bucket.AxisTime,
		BucketSpecRaw:    spec,
		ShardMaxBytes:    maxBytes,
		RetentionDepth:   &rd,
		MaxFutureBuckets: &mf,
	})
}

func (e *Env) BootstrapNumeric(width int64, maxBytes int64) {
	spec, _ := json.Marshal(map[string]int64{"width": width})
	e.MustBootstrap(store.ClusterConfig{
		Mode:          bucket.ModeRange,
		BucketAxis:    bucket.AxisNumeric,
		BucketSpecRaw: spec,
		ShardMaxBytes: maxBytes,
	})
}

func (e *Env) BootstrapHash(bucketCount int, maxBytes int64) {
	spec, _ := json.Marshal(map[string]any{"bucket_count": bucketCount, "hash_algo": bucket.HashAlgoXXHash64})
	e.MustBootstrap(store.ClusterConfig{
		Mode:          bucket.ModeHash,
		BucketAxis:    bucket.AxisHash,
		BucketSpecRaw: spec,
		ShardMaxBytes: maxBytes,
	})
}

func (e *Env) RegisterError() uuid.UUID {
	u := uuid.New()
	_, err := e.Store.RegisterShard(e.Ctx, u, fsm.RoleError, "postgres://error/db", "", fsm.StateActive)
	if err != nil {
		panic(err)
	}
	return u
}

func (e *Env) RegisterStandbys(n int) []uuid.UUID {
	out := make([]uuid.UUID, n)
	for i := 0; i < n; i++ {
		u := uuid.New()
		dsn := fmt.Sprintf("postgres://standby-%d/db", i)
		_, err := e.Store.RegisterShard(e.Ctx, u, fsm.RoleData, dsn, "", fsm.StateStandby)
		if err != nil {
			panic(err)
		}
		out[i] = u
	}
	return out
}

func (e *Env) MustSealRotate(bucketID string) {
	if err := e.Store.SealRotate(e.Ctx, bucketID); err != nil {
		panic(err)
	}
}

func (e *Env) ActiveUUID(bucketID string) uuid.UUID {
	sh, err := e.Store.ActiveForBucket(e.Ctx, bucketID)
	if err != nil {
		panic(err)
	}
	return sh.ShardUUID
}

func (e *Env) ShardsInBucket(bucketID string) []store.Shard {
	all, err := e.Store.ListShards(e.Ctx)
	if err != nil {
		panic(err)
	}
	var out []store.Shard
	for _, sh := range all {
		if sh.BucketID != nil && *sh.BucketID == bucketID {
			out = append(out, sh)
		}
	}
	return out
}
