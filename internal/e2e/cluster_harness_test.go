//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	shardmanv1 "github.com/tormoz70/shardman/api/gen/shardman/v1"
	"github.com/tormoz70/shardman/internal/agent"
	"github.com/tormoz70/shardman/internal/bucket"
	"github.com/tormoz70/shardman/internal/config"
	"github.com/tormoz70/shardman/internal/fsm"
	"github.com/tormoz70/shardman/internal/grpcapi"
	"github.com/tormoz70/shardman/internal/resolve"
	"github.com/tormoz70/shardman/internal/retention"
	"github.com/tormoz70/shardman/internal/seal"
	"github.com/tormoz70/shardman/internal/store"
	"github.com/tormoz70/shardman/internal/topology"
	"github.com/tormoz70/shardman/pkg/client"
)

const (
	e2eClusterKey = "e2e-load-key"
	shardMaxBytes = 48 * 1024
	payloadSize   = 6 * 1024
)

// Cluster is a multi-database e2e test harness with real Postgres shards.
type Cluster struct {
	t              *testing.T
	ctx            context.Context
	terminatePG    func()
	Store          *store.Store
	Client         *client.Client
	Internal       shardmanv1.InternalServiceClient
	Resolver       *resolve.Service
	SealSup        *seal.Supervisor
	RetSup         *retention.Supervisor
	FixedNow       time.Time
	shardDSNs      map[uuid.UUID]string
	agentCfgs      []config.AgentConfig
	errorShardUUID uuid.UUID
}

func skipIfNoE2E(t *testing.T) {
	t.Helper()
	if os.Getenv("SKIP_E2E") != "" {
		t.Skip("SKIP_E2E set")
	}
}

func newCluster(t *testing.T, dataShards int, withError bool) *Cluster {
	skipIfNoE2E(t)
	t.Helper()

	ctx := context.Background()
	dbNames := []string{"shardman_meta"}
	if withError {
		dbNames = append(dbNames, "shard_error")
	}
	for i := 0; i < dataShards; i++ {
		dbNames = append(dbNames, fmt.Sprintf("shard_data_%d", i))
	}

	dsns, terminate := startPostgresMulti(ctx, t, dbNames)
	t.Cleanup(terminate)

	fixedNow := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)

	st, err := store.New(ctx, dsns["shardman_meta"], store.Options{})
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	resolver := &resolve.Service{
		Store: st,
		Now:   func() time.Time { return fixedNow },
	}
	retSup := &retention.Supervisor{
		Store: st,
		Now:   func() time.Time { return fixedNow },
		Log:   slog.Default(),
	}
	sealSup := &seal.Supervisor{
		Store:        st,
		DrainTimeout: time.Millisecond,
		Log:          slog.Default(),
	}

	srv := &grpcapi.Server{
		Store:      st,
		ClusterKey: e2eClusterKey,
		Resolver:   resolver,
		SealSup:    sealSup,
		RetSup:     retSup,
		Broadcast:  topology.NewBroadcast(),
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	grpcapi.Register(gs, srv)
	go gs.Serve(lis)
	t.Cleanup(func() {
		gs.GracefulStop()
	})

	addr := lis.Addr().String()
	grpcConn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { grpcConn.Close() })

	c, err := client.Dial(ctx, addr, client.Options{ClusterKey: e2eClusterKey})
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	cl := &Cluster{
		t:           t,
		ctx:         ctx,
		terminatePG: terminate,
		Store:       st,
		Client:      c,
		Internal:    shardmanv1.NewInternalServiceClient(grpcConn),
		Resolver:    resolver,
		SealSup:     sealSup,
		RetSup:      retSup,
		FixedNow:    fixedNow,
		shardDSNs:   make(map[uuid.UUID]string),
	}

	if withError {
		if err := initShardSchema(ctx, dsns["shard_error"]); err != nil {
			t.Fatalf("error shard schema: %v", err)
		}
		errUUID := uuid.New()
		if _, err := c.RegisterShard(ctx, &shardmanv1.RegisterShardRequest{
			ShardUuid:    errUUID.String(),
			Dsn:          dsns["shard_error"],
			Role:         "error",
			StartupState: "active",
		}); err != nil {
			t.Fatalf("register error shard: %v", err)
		}
		cl.errorShardUUID = errUUID
		cl.shardDSNs[errUUID] = dsns["shard_error"]
		cl.agentCfgs = append(cl.agentCfgs, config.AgentConfig{
			PGDSN:           dsns["shard_error"],
			ShardUUID:       errUUID.String(),
			CoordinatorAddr: addr,
			ClusterKey:      e2eClusterKey,
			SizeSource:      "relations",
		})
	}

	for i := 0; i < dataShards; i++ {
		dbName := fmt.Sprintf("shard_data_%d", i)
		if err := initShardSchema(ctx, dsns[dbName]); err != nil {
			t.Fatalf("shard %d schema: %v", i, err)
		}
		u := uuid.New()
		if _, err := c.RegisterShard(ctx, &shardmanv1.RegisterShardRequest{
			ShardUuid:    u.String(),
			Dsn:          dsns[dbName],
			Role:         "data",
			StartupState: "standby",
		}); err != nil {
			t.Fatalf("register data shard %d: %v", i, err)
		}
		cl.shardDSNs[u] = dsns[dbName]
		cl.agentCfgs = append(cl.agentCfgs, config.AgentConfig{
			PGDSN:           dsns[dbName],
			ShardUUID:       u.String(),
			CoordinatorAddr: addr,
			ClusterKey:      e2eClusterKey,
			SizeSource:      "relations",
		})
	}

	return cl
}

func (c *Cluster) SetNow(t time.Time) {
	c.FixedNow = t
	c.Resolver.Now = func() time.Time { return t }
	c.RetSup.Now = func() time.Time { return t }
}

func (c *Cluster) BootstrapTime(retentionDepth, maxFuture int32) {
	c.bootstrap("range", "time", client.JSONSpec(map[string]string{"unit": "day"}), retentionDepth, maxFuture)
}

func (c *Cluster) BootstrapNumeric(width int64) {
	c.bootstrap("range", "numeric", client.JSONSpec(map[string]int64{"width": width}), 0, 0)
}

func (c *Cluster) BootstrapHash(bucketCount int) {
	c.bootstrap("hash", "hash", client.JSONSpec(map[string]any{
		"bucket_count": bucketCount,
		"hash_algo":    bucket.HashAlgoXXHash64,
	}), 0, 0)
}

func (c *Cluster) bootstrap(mode, axis string, specJSON []byte, retentionDepth, maxFuture int32) {
	c.t.Helper()
	req := &shardmanv1.BootstrapRequest{
		Mode:           mode,
		BucketAxis:     axis,
		BucketSpecJson: specJSON,
		ShardMaxBytes:  shardMaxBytes,
	}
	if retentionDepth > 0 || mode == "range" && axis == "time" {
		req.RetentionDepth = &retentionDepth
	}
	if axis == "time" {
		req.MaxFutureBuckets = &maxFuture
	}
	if _, err := c.Client.Bootstrap(c.ctx, req); err != nil {
		c.t.Fatalf("bootstrap: %v", err)
	}
}

func (c *Cluster) AgentTickAll() {
	c.t.Helper()
	for _, cfg := range c.agentCfgs {
		if err := agent.Tick(c.ctx, cfg, c.Internal); err != nil {
			c.t.Fatalf("agent tick %s: %v", cfg.ShardUUID, err)
		}
	}
}

func (c *Cluster) SealCycle() {
	c.t.Helper()
	c.SealSup.Tick(c.ctx)
	c.AgentTickAll()
	c.SealSup.Tick(c.ctx)
}

func (c *Cluster) bucketIDForKey(shardKey any) (string, error) {
	cfg, err := c.Store.GetConfig(c.ctx)
	if err != nil {
		return "", err
	}
	return cfg.BucketSpec.ID(shardKey)
}

func (c *Cluster) bucketDrainInProgress(bucketID string) bool {
	for _, sh := range c.ShardsInBucket(bucketID) {
		if sh.State == fsm.StateDraining {
			return true
		}
	}
	return false
}

func (c *Cluster) resolveWriteRetry(shardKey any) (*resolve.WriteResult, error) {
	c.t.Helper()
	for i := 0; i < 30; i++ {
		res, err := c.Resolver.ResolveWrite(c.ctx, shardKey)
		if err == nil {
			return res, nil
		}
		if errors.Is(err, store.ErrNotFound) {
			bid, bidErr := c.bucketIDForKey(shardKey)
			if bidErr == nil && c.bucketDrainInProgress(bid) {
				c.AgentTickAll()
				c.SealCycle()
				continue
			}
			return nil, err
		}
		return nil, err
	}
	return nil, fmt.Errorf("resolve write retries exhausted")
}

func (c *Cluster) insertRow(shardKey any) error {
	c.t.Helper()
	wr, err := c.resolveWriteRetry(shardKey)
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(c.ctx, wr.Endpoint)
	if err != nil {
		return err
	}
	defer conn.Close(c.ctx)

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte('x')
	}
	_, err = conn.Exec(c.ctx,
		`INSERT INTO load_items (shard_key, payload) VALUES ($1, $2)`,
		fmt.Sprint(shardKey), payload)
	return err
}

func (c *Cluster) FillRows(shardKey any, rows int) {
	c.t.Helper()
	for i := 0; i < rows; i++ {
		if err := c.insertRow(shardKey); err != nil {
			c.t.Fatalf("fill row %d key=%v: %v", i, shardKey, err)
		}
	}
}

// DriveVolumeSeal fills the active shard until volume seal completes.
// Returns promoted=true if standby was promoted, false if pool exhausted.
func (c *Cluster) DriveVolumeSeal(shardKey any) (promoted bool) {
	c.t.Helper()
	wr, err := c.resolveWriteRetry(shardKey)
	if err != nil {
		c.t.Fatalf("initial resolve: %v", err)
	}
	oldUUID := wr.UUID
	bucketID := wr.BucketID

	for iter := 0; iter < 100; iter++ {
		if c.bucketSealedNoActive(bucketID) {
			return false
		}

		if err := c.insertRow(shardKey); err != nil {
			c.t.Fatalf("insert: %v", err)
		}
		c.AgentTickAll()
		c.SealCycle()

		if c.bucketSealedNoActive(bucketID) {
			return false
		}
		active, err := c.Store.ActiveForBucket(c.ctx, bucketID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				if c.bucketDrainInProgress(bucketID) {
					continue
				}
				return false
			}
			c.t.Fatalf("active for bucket: %v", err)
		}
		if active.ShardUUID.String() != oldUUID {
			return true
		}
	}
	c.t.Fatalf("driveVolumeSeal: max iterations for bucket %s", bucketID)
	return false
}

func (c *Cluster) SealBucketVolume(bucketID string) (promoted bool) {
	c.t.Helper()
	active, err := c.Store.ActiveForBucket(c.ctx, bucketID)
	if err != nil {
		c.t.Fatalf("no active for bucket %s: %v", bucketID, err)
	}
	oldUUID := active.ShardUUID
	if err := c.Store.UpdateStats(c.ctx, oldUUID, shardMaxBytes); err != nil {
		c.t.Fatalf("update stats: %v", err)
	}

	for iter := 0; iter < 100; iter++ {
		if c.bucketSealedNoActive(bucketID) {
			return false
		}
		c.AgentTickAll()
		c.SealCycle()

		if c.bucketSealedNoActive(bucketID) {
			return false
		}
		cur, err := c.Store.ActiveForBucket(c.ctx, bucketID)
		if err == nil && cur.ShardUUID != oldUUID {
			return true
		}
	}
	c.t.Fatalf("sealBucketVolume: max iterations for bucket %s", bucketID)
	return false
}

func (c *Cluster) bucketSealedNoActive(bucketID string) bool {
	has, err := c.Store.HasActiveInBucket(c.ctx, bucketID)
	if err != nil || has {
		return false
	}
	for _, sh := range c.ShardsInBucket(bucketID) {
		if sh.State == fsm.StateSealed {
			return true
		}
	}
	return false
}

func (c *Cluster) ExpectUnavailable(shardKey any) {
	c.t.Helper()
	_, err := c.Client.ResolveWrite(c.ctx, shardKey)
	if status.Code(err) != codes.Unavailable {
		c.t.Fatalf("expected Unavailable for key=%v, got %v", shardKey, err)
	}
}

func (c *Cluster) ExpectErrorRoute(shardKey any, reason bucket.RouteReason) {
	c.t.Helper()
	res, err := c.Resolver.ResolveWrite(c.ctx, shardKey)
	if err != nil {
		c.t.Fatalf("resolve write: %v", err)
	}
	if res.Routing != bucket.RouteError || res.Reason != reason {
		c.t.Fatalf("expected error route %s, got routing=%s reason=%s", reason, res.Routing, res.Reason)
	}
}

func (c *Cluster) ExpectBucket(shardKey any, wantBucket string) *resolve.WriteResult {
	c.t.Helper()
	res, err := c.Resolver.ResolveWrite(c.ctx, shardKey)
	if err != nil {
		c.t.Fatalf("resolve write: %v", err)
	}
	if res.Routing != bucket.RouteBucket || res.BucketID != wantBucket {
		c.t.Fatalf("key=%v want bucket %s got %+v", shardKey, wantBucket, res)
	}
	return res
}

func (c *Cluster) CountRows(dsn string) int64 {
	c.t.Helper()
	conn, err := pgx.Connect(c.ctx, dsn)
	if err != nil {
		c.t.Fatal(err)
	}
	defer conn.Close(c.ctx)
	var n int64
	if err := conn.QueryRow(c.ctx, `SELECT COUNT(*) FROM load_items`).Scan(&n); err != nil {
		c.t.Fatal(err)
	}
	return n
}

func (c *Cluster) StandbyCount() int {
	c.t.Helper()
	n, err := c.Store.CountStandbyPool(c.ctx)
	if err != nil {
		c.t.Fatal(err)
	}
	return n
}

func (c *Cluster) ShardsInBucket(bucketID string) []store.Shard {
	c.t.Helper()
	all, err := c.Store.ListShards(c.ctx)
	if err != nil {
		c.t.Fatal(err)
	}
	var out []store.Shard
	for _, sh := range all {
		if sh.BucketID != nil && *sh.BucketID == bucketID {
			out = append(out, sh)
		}
	}
	return out
}

func (c *Cluster) RunRetentionAndClean() {
	c.t.Helper()
	if err := c.RetSup.Tick(c.ctx); err != nil {
		c.t.Fatalf("retention tick: %v", err)
	}
	for i := 0; i < 5; i++ {
		c.AgentTickAll()
	}
}

func initShardSchema(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS load_items (
			id         BIGSERIAL PRIMARY KEY,
			shard_key  TEXT NOT NULL,
			payload    BYTEA NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`)
	return err
}

func startPostgresMulti(ctx context.Context, t *testing.T, dbNames []string) (map[string]string, func()) {
	t.Helper()
	if len(dbNames) == 0 {
		t.Fatal("dbNames required")
	}
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "shardman",
			"POSTGRES_PASSWORD": "shardman",
			"POSTGRES_DB":       dbNames[0],
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(3 * time.Minute),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("postgres container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatal(err)
	}

	dsns := make(map[string]string, len(dbNames))
	for _, name := range dbNames {
		dsns[name] = fmt.Sprintf("postgres://shardman:shardman@%s:%s/%s?sslmode=disable",
			host, port.Port(), name)
	}

	if len(dbNames) > 1 {
		admin, err := pgx.Connect(ctx, dsns[dbNames[0]])
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range dbNames[1:] {
			if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, name)); err != nil {
				admin.Close(ctx)
				t.Fatalf("create database %s: %v", name, err)
			}
		}
		admin.Close(ctx)
	}

	terminate := func() { _ = container.Terminate(ctx) }
	return dsns, terminate
}

func countStates(shards []store.Shard) (sealed, active int) {
	for _, sh := range shards {
		switch sh.State {
		case fsm.StateSealed:
			sealed++
		case fsm.StateActive:
			active++
		}
	}
	return
}
