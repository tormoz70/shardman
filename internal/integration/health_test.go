//go:build integration

package integration

import (
	"log/slog"
	"testing"
	"time"

	"github.com/tormoz70/shardman/internal/health"
)

func TestStaleActiveFailoverIntegration(t *testing.T) {
	env := OpenEnv(t)
	defer env.Close()

	env.BootstrapHash(4, 1_000_000)
	env.RegisterError()
	standbys := env.RegisterStandbys(2)
	_ = standbys

	wr, err := env.Resolver.ResolveWrite(env.Ctx, "key-a")
	if err != nil {
		t.Fatal(err)
	}
	bucketID := wr.BucketID
	active := env.ActiveUUID(bucketID)

	if err := env.Store.SetLastSeenAt(env.Ctx, active, time.Now().Add(-5*time.Minute)); err != nil {
		t.Fatal(err)
	}

	if _, err := env.Store.ActiveForBucket(env.Ctx, bucketID); err == nil {
		t.Fatal("expected stale active excluded")
	}

	healthSup := &health.Supervisor{Store: env.Store, Interval: time.Second, Log: slog.Default()}
	healthSup.Tick(env.Ctx)

	wr2, err := env.Resolver.ResolveWrite(env.Ctx, "key-a")
	if err != nil {
		t.Fatal(err)
	}
	if wr2.UUID == active.String() {
		t.Fatal("expected new active after failover")
	}
}
