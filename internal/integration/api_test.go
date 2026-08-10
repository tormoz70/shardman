//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	shardmanv1 "github.com/tormoz70/shardman/api/gen/shardman/v1"
	"github.com/tormoz70/shardman/internal/bucket"
	"github.com/tormoz70/shardman/internal/fsm"
	"github.com/tormoz70/shardman/internal/grpcapi"
	"github.com/tormoz70/shardman/internal/topology"
)

func newGRPCServer(e *Env) *grpcapi.Server {
	return &grpcapi.Server{
		Store:      e.Store,
		ClusterKey: clusterKey,
		Resolver:   e.Resolver,
		RetSup:     e.Retention,
		Broadcast:  topology.NewBroadcast(),
	}
}

func adminCtx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-cluster-key", clusterKey)
}

func TestAPIBootstrapAnd409(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()
	srv := newGRPCServer(e)
	_, admin := dialClients(t, srv)
	ctx := adminCtx(context.Background())

	req := &shardmanv1.BootstrapRequest{
		Mode:           "hash",
		BucketAxis:     "hash",
		BucketSpecJson: []byte(`{"bucket_count":8,"hash_algo":"xxhash64"}`),
		ShardMaxBytes:  1024,
	}
	if _, err := admin.Bootstrap(ctx, req); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if _, err := admin.Bootstrap(ctx, req); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
}

func TestAPIUnauthorizedBootstrap(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()
	srv := newGRPCServer(e)
	_, admin := dialClients(t, srv)

	_, err := admin.Bootstrap(context.Background(), &shardmanv1.BootstrapRequest{
		Mode:           "range",
		BucketAxis:     "numeric",
		BucketSpecJson: []byte(`{"width":1000}`),
		ShardMaxBytes:  1024,
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("status %v", err)
	}
}

func TestAPIResolveWrite503(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()
	e.BootstrapNumeric(1000, 1024)
	srv := newGRPCServer(e)
	resolveClient, _ := dialClients(t, srv)

	v, err := structpbNew(500)
	if err != nil {
		t.Fatal(err)
	}
	_, err = resolveClient.Write(context.Background(), &shardmanv1.ResolveWriteRequest{ShardKey: v})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("status %v", err)
	}
}

func TestAPIResolveWriteAndSealRotate(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()
	maxBytes := int64(100)
	e.BootstrapNumeric(1000, maxBytes)
	e.RegisterStandbys(2)
	srv := newGRPCServer(e)
	resolveClient, admin := dialClients(t, srv)

	v, _ := structpbNew(500)
	wr, err := resolveClient.Write(context.Background(), &shardmanv1.ResolveWriteRequest{ShardKey: v})
	if err != nil {
		t.Fatal(err)
	}
	if wr.GetBucketId() != "n0" || wr.GetRouting() != string(bucket.RouteBucket) {
		t.Fatalf("write %+v", wr)
	}

	if err := e.Store.UpdateStats(e.Ctx, e.ActiveUUID("n0"), maxBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.SealRotate(adminCtx(e.Ctx), &shardmanv1.SealRotateRequest{BucketId: "n0"}); err != nil {
		t.Fatal(err)
	}

	rr, err := resolveClient.Read(context.Background(), &shardmanv1.ResolveReadRequest{ShardKey: v})
	if err != nil {
		t.Fatal(err)
	}
	if len(rr.GetShards()) != 2 {
		t.Fatalf("read shards %+v", rr)
	}
}

func TestAPIRegisterShard(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()
	e.BootstrapNumeric(1000, 1024)
	srv := newGRPCServer(e)
	_, admin := dialClients(t, srv)

	u := uuid.New()
	_, err := admin.RegisterShard(adminCtx(e.Ctx), &shardmanv1.RegisterShardRequest{
		ShardUuid:    u.String(),
		Dsn:          "postgres://api/db",
		Role:         "data",
		StartupState: "standby",
	})
	if err != nil {
		t.Fatal(err)
	}
	sh, err := e.Store.GetShardByUUID(e.Ctx, u)
	if err != nil {
		t.Fatal(err)
	}
	if sh.State != fsm.StateStandby || sh.BucketID != nil {
		t.Fatalf("shard %+v", sh)
	}
}

func TestAPIBucketShardsList(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()
	e.BootstrapNumeric(1000, 1024)
	e.RegisterStandbys(1)
	_, err := e.Resolver.ResolveWrite(e.Ctx, 500)
	if err != nil {
		t.Fatal(err)
	}
	srv := newGRPCServer(e)
	resolveClient, _ := dialClients(t, srv)

	resp, err := resolveClient.ListBucketShards(context.Background(), &shardmanv1.ListBucketShardsRequest{BucketId: "n0"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetBucketId() != "n0" || len(resp.GetShards()) == 0 {
		t.Fatalf("resp %+v", resp)
	}
}

func TestTopologyVersionBump(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()
	e.BootstrapNumeric(1000, 1024)
	srv := newGRPCServer(e)
	topoClient := dialTopology(t, srv)

	before, err := topoClient.Get(context.Background(), &shardmanv1.GetTopologyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterStandbys(1)
	after, err := topoClient.Get(context.Background(), &shardmanv1.GetTopologyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if after.TopologyVersion <= before.TopologyVersion {
		t.Fatalf("version before=%d after=%d", before.TopologyVersion, after.TopologyVersion)
	}
}

func TestRetentionSkipsActiveBucket(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()
	e.BootstrapTime(1, 0, 1024)
	e.RegisterError()
	e.RegisterStandbys(5)

	wr, err := e.Resolver.ResolveWrite(e.Ctx, "2026-05-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Retention.Tick(e.Ctx); err != nil {
		t.Fatal(err)
	}
	for _, sh := range e.ShardsInBucket(wr.BucketID) {
		if sh.State == fsm.StateCleaning {
			t.Fatalf("active bucket cleaned: %+v", sh)
		}
	}
}
