//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	shardmanv1 "github.com/tormoz70/shardman/api/gen/shardman/v1"
	"github.com/tormoz70/shardman/pkg/client"
)

func TestE2EGRPCBootstrapResolve(t *testing.T) {
	if os.Getenv("SKIP_E2E") != "" {
		t.Skip("SKIP_E2E set")
	}
	ctx := context.Background()
	metaDSN, terminateMeta := startPostgres(ctx, t, "shardman_meta")
	defer terminateMeta()

	st, err := storeNew(ctx, metaDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	addr, stop := startTestServer(ctx, t, metaDSN)
	defer stop()

	c, err := client.Dial(ctx, addr, client.Options{ClusterKey: "e2e-key"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	rd := int32(1)
	mf := int32(0)
	_, err = c.Bootstrap(ctx, &shardmanv1.BootstrapRequest{
		Mode:             "range",
		BucketAxis:       "time",
		BucketSpecJson:   client.JSONSpec(map[string]string{"unit": "month"}),
		ShardMaxBytes:    1 << 30,
		RetentionDepth:   &rd,
		MaxFutureBuckets: &mf,
	})
	if err != nil {
		t.Fatal(err)
	}

	uErr := uuid.New()
	_, err = c.RegisterShard(ctx, &shardmanv1.RegisterShardRequest{
		ShardUuid:    uErr.String(),
		Dsn:          "postgres://error/db",
		Role:         "error",
		StartupState: "active",
	})
	if err != nil {
		t.Fatal(err)
	}
	uData := uuid.New()
	_, err = c.RegisterShard(ctx, &shardmanv1.RegisterShardRequest{
		ShardUuid:    uData.String(),
		Dsn:          "postgres://data/db",
		Role:         "data",
		StartupState: "standby",
	})
	if err != nil {
		t.Fatal(err)
	}

	wr, err := c.ResolveWrite(ctx, "2026-08-01T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if wr.Endpoint == "" {
		t.Fatal("empty endpoint")
	}

	topo, err := c.GetTopology(ctx)
	if err != nil || topo.TopologyVersion < 1 {
		t.Fatalf("topology %+v err=%v", topo, err)
	}
}

func startPostgres(ctx context.Context, t *testing.T, db string) (string, func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "shardman",
			"POSTGRES_PASSWORD": "shardman",
			"POSTGRES_DB":       db,
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(2 * time.Minute),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("postgres container: %v", err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatal(err)
	}
	dsn := fmt.Sprintf("postgres://shardman:shardman@%s:%s/%s?sslmode=disable", host, port.Port(), db)
	return dsn, func() { _ = c.Terminate(ctx) }
}
