//go:build integration

package integration

import (
	"encoding/json"
	"testing"

	"github.com/tormoz70/shardman/internal/bucket"
	"github.com/tormoz70/shardman/internal/fsm"
)

const hashKeyA = "integration-key-a"
const hashKeyB = "integration-key-b"

func hashBucketID(key string) string {
	spec, _ := bucket.ParseSpec(bucket.AxisHash, json.RawMessage(`{"bucket_count":8,"hash_algo":"xxhash64"}`))
	id, err := spec.ID(key)
	if err != nil {
		panic(err)
	}
	return id
}

func TestHashStableBucketID(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.BootstrapHash(8, 1024)
	e.RegisterError()
	e.RegisterStandbys(1)

	want := hashBucketID(hashKeyA)

	r1, err := e.Resolver.ResolveWrite(e.Ctx, hashKeyA)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := e.Resolver.ResolveWrite(e.Ctx, hashKeyA)
	if err != nil {
		t.Fatal(err)
	}
	if r1.BucketID != want || r2.BucketID != want {
		t.Fatalf("want %s got %s %s", want, r1.BucketID, r2.BucketID)
	}
}

func TestHashTwoKeys(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.BootstrapHash(8, 1024)
	e.RegisterError()
	e.RegisterStandbys(2)

	wantA := hashBucketID(hashKeyA)
	wantB := hashBucketID(hashKeyB)

	rA, err := e.Resolver.ResolveWrite(e.Ctx, hashKeyA)
	if err != nil {
		t.Fatal(err)
	}
	rB, err := e.Resolver.ResolveWrite(e.Ctx, hashKeyB)
	if err != nil {
		t.Fatal(err)
	}
	if rA.BucketID != wantA || rB.BucketID != wantB {
		t.Fatalf("A=%s want %s B=%s want %s", rA.BucketID, wantA, rB.BucketID, wantB)
	}
	if wantA == wantB {
		t.Fatalf("test keys must map to different buckets for this case")
	}
}

func TestHashSealSameBucketCount(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	maxBytes := int64(100)
	e.BootstrapHash(8, maxBytes)
	e.RegisterError()
	e.RegisterStandbys(2)

	want := hashBucketID(hashKeyA)
	_, err := e.Resolver.ResolveWrite(e.Ctx, hashKeyA)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := e.Store.GetConfig(e.Ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BucketSpec.BucketCount != 8 {
		t.Fatalf("bucket_count %d", cfg.BucketSpec.BucketCount)
	}

	if err := e.Store.UpdateStats(e.Ctx, e.ActiveUUID(want), maxBytes); err != nil {
		t.Fatal(err)
	}
	e.MustSealRotate(want)

	shards := e.ShardsInBucket(want)
	var sealed, active int
	for _, sh := range shards {
		switch sh.State {
		case fsm.StateSealed:
			sealed++
		case fsm.StateActive:
			active++
		}
	}
	if sealed != 1 || active != 1 {
		t.Fatalf("sealed=%d active=%d", sealed, active)
	}

	cfg2, _ := e.Store.GetConfig(e.Ctx)
	if cfg2.BucketSpec.BucketCount != 8 {
		t.Fatalf("bucket_count changed")
	}
}

func TestHashInvalidKeyErrorRoute(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.BootstrapHash(8, 1024)
	e.RegisterError()
	e.RegisterStandbys(1)

	res, err := e.Resolver.ResolveWrite(e.Ctx, map[string]int{"bad": 1})
	if err != nil {
		t.Fatal(err)
	}
	if res.Routing != bucket.RouteError || res.Reason != bucket.ReasonInvalidKey {
		t.Fatalf("got %+v", res)
	}
}
