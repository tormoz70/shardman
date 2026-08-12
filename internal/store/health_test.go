//go:build integration

package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/tormoz70/shardman/internal/bucket"
	"github.com/tormoz70/shardman/internal/fsm"
)

func TestStaleActiveExclusionAndFailover(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t, 2*time.Second)
	defer st.Close()

	spec, _ := json.Marshal(map[string]any{"bucket_count": 4, "hash_algo": bucket.HashAlgoXXHash64})
	if err := st.Bootstrap(ctx, ClusterConfig{
		Mode:          bucket.ModeHash,
		BucketAxis:    bucket.AxisHash,
		BucketSpecRaw: spec,
		ShardMaxBytes: 1_000_000,
	}); err != nil {
		t.Fatal(err)
	}

	activeID := uuid.New()
	standbyID := uuid.New()
	if _, err := st.RegisterShard(ctx, activeID, fsm.RoleData, "postgres://active/db", "", fsm.StateStandby); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RegisterShard(ctx, standbyID, fsm.RoleData, "postgres://standby/db", "", fsm.StateStandby); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PromoteStandbyToActive(ctx, "h0"); err != nil {
		t.Fatal(err)
	}

	active, err := st.ActiveForBucket(ctx, "h0")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetLastSeenAt(ctx, active.ShardUUID, time.Now().Add(-5*time.Minute)); err != nil {
		t.Fatal(err)
	}

	if _, err := st.ActiveForBucket(ctx, "h0"); err != ErrNotFound {
		t.Fatalf("expected stale active excluded, got %v", err)
	}
	stale, err := st.StaleActiveForBucket(ctx, "h0")
	if err != nil {
		t.Fatal(err)
	}
	if stale.ID != active.ID {
		t.Fatalf("stale id=%d want %d", stale.ID, active.ID)
	}

	if err := st.SealRotate(ctx, "h0"); err != nil {
		t.Fatal(err)
	}
	newActive, err := st.ActiveForBucket(ctx, "h0")
	if err != nil {
		t.Fatal(err)
	}
	if newActive.ShardUUID == active.ShardUUID {
		t.Fatal("expected promoted standby after stale failover")
	}
}

func TestAutoPromoteSkipsDrainingBucket(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t, time.Minute)
	defer st.Close()

	spec, _ := json.Marshal(map[string]int64{"width": 1000})
	if err := st.Bootstrap(ctx, ClusterConfig{
		Mode:          bucket.ModeRange,
		BucketAxis:    bucket.AxisNumeric,
		BucketSpecRaw: spec,
		ShardMaxBytes: 100,
	}); err != nil {
		t.Fatal(err)
	}

	activeID := uuid.New()
	standbyID := uuid.New()
	if _, err := st.RegisterShard(ctx, activeID, fsm.RoleData, "postgres://active/db", "", fsm.StateStandby); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RegisterShard(ctx, standbyID, fsm.RoleData, "postgres://standby/db", "", fsm.StateStandby); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PromoteStandbyToActive(ctx, "n0"); err != nil {
		t.Fatal(err)
	}
	if err := st.BeginDrain(ctx, "n0"); err != nil {
		t.Fatal(err)
	}

	promoted, err := st.AutoPromoteIfNoActive(ctx, "n0")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found during drain, got promoted=%v err=%v", promoted, err)
	}
	if promoted != nil {
		t.Fatalf("expected no promotion during drain, got %+v", promoted)
	}

	standbys, err := st.CountStandbyPool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if standbys != 1 {
		t.Fatalf("standby pool=%d want 1", standbys)
	}
}

func TestErrorShardNotFilteredByHeartbeat(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t, time.Second)
	defer st.Close()

	spec, _ := json.Marshal(map[string]any{"bucket_count": 4, "hash_algo": bucket.HashAlgoXXHash64})
	if err := st.Bootstrap(ctx, ClusterConfig{
		Mode:          bucket.ModeHash,
		BucketAxis:    bucket.AxisHash,
		BucketSpecRaw: spec,
		ShardMaxBytes: 1_000_000,
	}); err != nil {
		t.Fatal(err)
	}
	errID := uuid.New()
	if _, err := st.RegisterShard(ctx, errID, fsm.RoleError, "postgres://error/db", "", fsm.StateActive); err != nil {
		t.Fatal(err)
	}
	if err := st.SetLastSeenAt(ctx, errID, time.Now().Add(-5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	sh, err := st.GetErrorShard(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if sh.ShardUUID != errID {
		t.Fatalf("error shard uuid=%s", sh.ShardUUID)
	}
}

func newTestStore(t *testing.T, heartbeat time.Duration) *Store {
	t.Helper()
	dsn := testDSN(t)
	st, err := New(context.Background(), dsn, Options{HeartbeatTimeout: heartbeat, MaxConns: 4})
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := st.ResetSchema(context.Background()); err != nil {
		st.Close()
		t.Fatalf("reset: %v", err)
	}
	return st
}

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := getenv("METADATA_PG_DSN", "")
	if dsn == "" {
		t.Skip("METADATA_PG_DSN not set")
	}
	return dsn
}

func getenv(k, def string) string {
	if v := osGetenv(k); v != "" {
		return v
	}
	return def
}

// osGetenv is a thin wrapper for test portability.
func osGetenv(k string) string {
	return os.Getenv(k)
}
