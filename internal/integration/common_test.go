//go:build integration

package integration

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/tormoz70/shardman/internal/bucket"
	"github.com/tormoz70/shardman/internal/fsm"
	"github.com/tormoz70/shardman/internal/store"
)

func TestCommonBootstrapTwice(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.BootstrapHash(8, 1024)
	if err := e.Store.Bootstrap(e.Ctx, store.ClusterConfig{
		Mode:          bucket.ModeHash,
		BucketAxis:    bucket.AxisHash,
		BucketSpecRaw: []byte(`{"bucket_count":8,"hash_algo":"xxhash64"}`),
		ShardMaxBytes: 1024,
	}); !errors.Is(err, store.ErrAlreadyBootstrapped) {
		t.Fatalf("expected already bootstrapped, got %v", err)
	}
}

func TestCommonNoStandby503(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.BootstrapNumeric(1000, 1024)
	_, err := e.Resolver.ResolveWrite(e.Ctx, 500)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestCommonPromoteAndRead(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.BootstrapNumeric(1000, 1024)
	e.RegisterStandbys(1)

	res, err := e.Resolver.ResolveWrite(e.Ctx, 500)
	if err != nil {
		t.Fatal(err)
	}
	if res.Routing != bucket.RouteBucket || res.BucketID != "n0" || res.State != fsm.StateActive {
		t.Fatalf("write: %+v", res)
	}

	read, err := e.Resolver.ResolveRead(e.Ctx, 500)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Shards) != 1 || read.Shards[0].State != fsm.StateActive {
		t.Fatalf("read: %+v", read)
	}
}

func TestCommonSealRotateSameBucket(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	maxBytes := int64(100)
	e.BootstrapNumeric(1000, maxBytes)
	e.RegisterStandbys(2)

	res, err := e.Resolver.ResolveWrite(e.Ctx, 500)
	if err != nil {
		t.Fatal(err)
	}
	oldUUID := res.UUID

	if err := e.Store.UpdateStats(e.Ctx, e.ActiveUUID("n0"), maxBytes); err != nil {
		t.Fatal(err)
	}
	e.MustSealRotate("n0")

	shards := e.ShardsInBucket("n0")
	var sealed, active int
	for _, sh := range shards {
		if sh.State == fsm.StateSealed {
			sealed++
			if sh.ShardUUID.String() != oldUUID {
				t.Fatalf("expected sealed old active")
			}
		}
		if sh.State == fsm.StateActive {
			active++
		}
	}
	if sealed != 1 || active != 1 {
		t.Fatalf("sealed=%d active=%d shards=%v", sealed, active, shards)
	}

	read, err := e.Resolver.ResolveRead(e.Ctx, 500)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Shards) != 2 {
		t.Fatalf("read should return sealed+active: %+v", read)
	}
}

func TestCommonHotAddStandbyAfterSeal(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	maxBytes := int64(100)
	e.BootstrapNumeric(1000, maxBytes)
	e.RegisterStandbys(2)

	_, err := e.Resolver.ResolveWrite(e.Ctx, 500)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Store.UpdateStats(e.Ctx, e.ActiveUUID("n0"), maxBytes); err != nil {
		t.Fatal(err)
	}
	e.MustSealRotate("n0")

	e.RegisterStandbys(1)
	if err := e.Store.UpdateStats(e.Ctx, e.ActiveUUID("n0"), maxBytes); err != nil {
		t.Fatal(err)
	}
	if err := e.Store.SealRotate(e.Ctx, "n0"); err != nil {
		t.Fatalf("second seal failed: %v", err)
	}
}

func TestCommonSecondErrorConflict(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.BootstrapNumeric(1000, 1024)
	e.RegisterError()
	_, err := e.Store.RegisterShard(e.Ctx, uuid.New(), fsm.RoleError, "postgres://e2/db", "", fsm.StateActive)
	if !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
}
