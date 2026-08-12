//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/tormoz70/shardman/internal/bucket"
	"github.com/tormoz70/shardman/internal/fsm"
)

const hashBucketCount = 4

func hashSpec() bucket.Spec {
	spec, _ := bucket.ParseSpec(bucket.AxisHash, json.RawMessage(fmt.Sprintf(
		`{"bucket_count":%d,"hash_algo":"xxhash64"}`, hashBucketCount)))
	return spec
}

func findKeysForDistinctBuckets(t *testing.T, n int) []string {
	t.Helper()
	spec := hashSpec()
	seen := make(map[string]string)
	for i := 0; len(seen) < n; i++ {
		key := fmt.Sprintf("e2e-hash-key-%d", i)
		bid, err := spec.ID(key)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := seen[bid]; !ok {
			seen[bid] = key
		}
	}
	out := make([]string, 0, n)
	for _, key := range seen {
		out = append(out, key)
	}
	return out
}

func bucketIDForKey(t *testing.T, key string) string {
	t.Helper()
	bid, err := hashSpec().ID(key)
	if err != nil {
		t.Fatal(err)
	}
	return bid
}

func TestHashStableBucketID(t *testing.T) {
	cl := newCluster(t, 2, true)
	cl.BootstrapHash(hashBucketCount)

	key := "integration-key-a"
	want := bucketIDForKey(t, key)

	r1 := cl.ExpectBucket(key, want)
	r2 := cl.ExpectBucket(key, want)
	if r1.Endpoint != r2.Endpoint {
		t.Fatalf("endpoints differ: %s vs %s", r1.Endpoint, r2.Endpoint)
	}
}

func TestHashTwoKeysDifferentBuckets(t *testing.T) {
	cl := newCluster(t, 2, true)
	cl.BootstrapHash(hashBucketCount)

	keys := findKeysForDistinctBuckets(t, 2)
	wantA := bucketIDForKey(t, keys[0])
	wantB := bucketIDForKey(t, keys[1])
	if wantA == wantB {
		t.Fatal("test keys must map to different buckets")
	}

	cl.ExpectBucket(keys[0], wantA)
	cl.ExpectBucket(keys[1], wantB)
}

func TestHashLowercaseNormalization(t *testing.T) {
	cl := newCluster(t, 1, true)
	cl.BootstrapHash(hashBucketCount)

	want := bucketIDForKey(t, "foo")
	cl.ExpectBucket("Foo", want)
	cl.ExpectBucket("FOO", want)
	cl.ExpectBucket("foo", want)
}

func TestHashInvalidKeyErrorRoute(t *testing.T) {
	cl := newCluster(t, 1, true)
	cl.BootstrapHash(hashBucketCount)

	cl.ExpectErrorRoute(map[string]int{"bad": 1}, bucket.ReasonInvalidKey)
}

func TestHashVolumeFillInBucket(t *testing.T) {
	cl := newCluster(t, 2, true)
	cl.BootstrapHash(hashBucketCount)

	key := findKeysForDistinctBuckets(t, 1)[0]
	want := bucketIDForKey(t, key)

	cl.FillRows(key, 5)
	if !cl.DriveVolumeSeal(key) {
		t.Fatal("expected standby promotion")
	}

	cfg, err := cl.Store.GetConfig(cl.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BucketSpec.BucketCount != hashBucketCount {
		t.Fatalf("bucket_count changed to %d", cfg.BucketSpec.BucketCount)
	}

	shards := cl.ShardsInBucket(want)
	sealed, active := countStates(shards)
	if sealed != 1 || active != 1 {
		t.Fatalf("sealed=%d active=%d", sealed, active)
	}

	read, err := cl.Resolver.ResolveRead(cl.ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Shards) != 2 {
		t.Fatalf("read shards: %+v", read)
	}

	cl.DriveVolumeSeal(key)
	cl.ExpectUnavailable(key)
}

func TestHashPoolExhaustedFourthBucket(t *testing.T) {
	cl := newCluster(t, 3, true)
	cl.BootstrapHash(hashBucketCount)

	keys := findKeysForDistinctBuckets(t, 4)
	for i := 0; i < 3; i++ {
		cl.FillRows(keys[i], 5)
	}
	if cl.StandbyCount() != 0 {
		t.Fatalf("expected 0 standby after 3 buckets, got %d", cl.StandbyCount())
	}
	for i := 0; i < 3; i++ {
		if cl.DriveVolumeSeal(keys[i]) {
			t.Fatalf("bucket %s should seal without promotion", keys[i])
		}
	}

	cl.ExpectUnavailable(keys[3])
}

func TestHashVolumeFillNoStandby503(t *testing.T) {
	cl := newCluster(t, 1, true)
	cl.BootstrapHash(hashBucketCount)

	key := findKeysForDistinctBuckets(t, 1)[0]
	cl.FillRows(key, 5)
	cl.DriveVolumeSeal(key)
	cl.ExpectUnavailable(key)
}

func TestHashSealPreservesBucketCount(t *testing.T) {
	cl := newCluster(t, 2, true)
	cl.BootstrapHash(hashBucketCount)

	key := findKeysForDistinctBuckets(t, 1)[0]
	cl.FillRows(key, 5)
	cl.DriveVolumeSeal(key)

	cfg, err := cl.Store.GetConfig(cl.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BucketSpec.BucketCount != hashBucketCount {
		t.Fatalf("bucket_count must stay %d", hashBucketCount)
	}

	shards := cl.ShardsInBucket(bucketIDForKey(t, key))
	for _, sh := range shards {
		if sh.State != fsm.StateSealed && sh.State != fsm.StateActive {
			t.Fatalf("unexpected state %s", sh.State)
		}
	}
}
