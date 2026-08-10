//go:build integration

package integration

import (
	"testing"

	"github.com/tormoz70/shardman/internal/fsm"
)

func TestNumericTwoBuckets(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.BootstrapNumeric(1000, 1024)
	e.RegisterStandbys(2)

	r0, err := e.Resolver.ResolveWrite(e.Ctx, 500)
	if err != nil {
		t.Fatal(err)
	}
	if r0.BucketID != "n0" {
		t.Fatalf("n0: %+v", r0)
	}

	r1, err := e.Resolver.ResolveWrite(e.Ctx, 1500)
	if err != nil {
		t.Fatal(err)
	}
	if r1.BucketID != "n1" {
		t.Fatalf("n1: %+v", r1)
	}
}

func TestNumericSealOnlyOneBucket(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	maxBytes := int64(100)
	e.BootstrapNumeric(1000, maxBytes)
	e.RegisterStandbys(3)

	_, err := e.Resolver.ResolveWrite(e.Ctx, 500)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.Resolver.ResolveWrite(e.Ctx, 1500)
	if err != nil {
		t.Fatal(err)
	}

	if err := e.Store.UpdateStats(e.Ctx, e.ActiveUUID("n0"), maxBytes); err != nil {
		t.Fatal(err)
	}
	e.MustSealRotate("n0")

	n1, err := e.Store.ActiveForBucket(e.Ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	if n1.State != fsm.StateActive {
		t.Fatalf("n1 should stay active: %s", n1.State)
	}
	n0Shards := e.ShardsInBucket("n0")
	if len(n0Shards) != 2 {
		t.Fatalf("n0 should have sealed+active: %d", len(n0Shards))
	}
}

func TestNumericRetentionNoOp(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.BootstrapNumeric(1000, 1024)
	e.RegisterStandbys(1)
	_, err := e.Resolver.ResolveWrite(e.Ctx, 500)
	if err != nil {
		t.Fatal(err)
	}

	if err := e.Retention.Tick(e.Ctx); err != nil {
		t.Fatal(err)
	}
	sh, err := e.Store.ActiveForBucket(e.Ctx, "n0")
	if err != nil || sh.State != fsm.StateActive {
		t.Fatalf("retention should not touch numeric: %+v %v", sh, err)
	}
}
