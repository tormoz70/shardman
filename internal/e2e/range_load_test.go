//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/tormoz70/shardman/internal/bucket"
	"github.com/tormoz70/shardman/internal/fsm"
)

func TestTimeNoRotation503(t *testing.T) {
	// retention_depth=3, 3 data shards, no spare standby.
	cl := newCluster(t, 3, true)
	cl.BootstrapTime(3, 0)
	cl.SetNow(time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC))

	days := []string{"2026-08-10", "2026-08-11", "2026-08-12"}
	for _, day := range days {
		cl.FillRows(day, 5)
	}

	for _, day := range days {
		cl.DriveVolumeSeal(day)
	}

	for _, day := range days {
		cl.ExpectUnavailable(day)
	}

	cl.ExpectErrorRoute("2026-08-13", bucket.ReasonFutureDisallowed)
}

func TestTimeRetentionRing(t *testing.T) {
	// retention_depth=2, 3 data + error.
	cl := newCluster(t, 3, true)
	cl.BootstrapTime(2, 0)

	cl.SetNow(time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC))
	cl.FillRows("2026-08-10", 5)
	cl.FillRows("2026-08-11", 5)

	cl.SetNow(time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC))
	cl.FillRows("2026-08-12", 5)

	if cl.StandbyCount() != 0 {
		t.Fatalf("expected 0 standby after 3 buckets filled, got %d", cl.StandbyCount())
	}

	// Bucket 2026-08-10 is evicted for writes at Now=Aug 12; seal active directly (no standby left).
	if err := cl.Client.SealRotate(cl.ctx, "2026-08-10"); err != nil {
		t.Fatalf("seal rotate: %v", err)
	}

	shards := cl.ShardsInBucket("2026-08-10")
	sealed, active := countStates(shards)
	if sealed == 0 || active > 0 {
		t.Fatalf("bucket 2026-08-10 should be sealed-only, shards=%v", shards)
	}

	var recycledDSN string
	for _, sh := range shards {
		if sh.State == fsm.StateSealed {
			recycledDSN = sh.DSN
			break
		}
	}
	beforeClean := cl.CountRows(recycledDSN)
	if beforeClean == 0 {
		t.Fatal("expected data on sealed shard before retention clean")
	}

	cl.RunRetentionAndClean()

	if cl.CountRows(recycledDSN) != 0 {
		t.Fatalf("expected truncated shard after clean, rows=%d", cl.CountRows(recycledDSN))
	}
	if cl.StandbyCount() != 1 {
		t.Fatalf("expected 1 standby after recycle, got %d", cl.StandbyCount())
	}

	cl.ExpectErrorRoute("2026-08-10", bucket.ReasonEvicted)

	cl.FillRows("2026-08-11", 2)
	cl.FillRows("2026-08-12", 2)
}

func TestTimeVolumeFillInBucket(t *testing.T) {
	cl := newCluster(t, 2, true)
	cl.BootstrapTime(3, 0)
	cl.SetNow(time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC))

	day := "2026-08-12"
	cl.FillRows(day, 5)

	promoted := cl.DriveVolumeSeal(day)
	if !promoted {
		t.Fatal("expected standby promotion after volume seal")
	}

	shards := cl.ShardsInBucket("2026-08-12")
	sealed, active := countStates(shards)
	if sealed != 1 || active != 1 {
		t.Fatalf("want sealed+active, got sealed=%d active=%d shards=%v", sealed, active, shards)
	}

	read, err := cl.Resolver.ResolveRead(cl.ctx, day)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Shards) != 2 {
		t.Fatalf("read should return sealed+active: %+v", read)
	}

	cl.FillRows(day, 3)
	cl.DriveVolumeSeal(day)
	cl.ExpectUnavailable(day)
}

func TestNumericThreeBuckets503(t *testing.T) {
	cl := newCluster(t, 3, false)
	cl.BootstrapNumeric(1000)

	keys := []any{int64(500), int64(1500), int64(2500)}
	wantBuckets := []string{"n0", "n1", "n2"}
	for i, k := range keys {
		cl.ExpectBucket(k, wantBuckets[i])
		cl.FillRows(k, 5)
	}

	for _, k := range keys {
		cl.DriveVolumeSeal(k)
	}

	for _, k := range keys {
		cl.ExpectUnavailable(k)
	}

	cl.ExpectUnavailable(int64(3500))
}

func TestNumericVolumeFillInBucket(t *testing.T) {
	cl := newCluster(t, 2, false)
	cl.BootstrapNumeric(1000)

	key := int64(500)
	cl.FillRows(key, 5)

	if !cl.DriveVolumeSeal(key) {
		t.Fatal("expected promotion")
	}

	shards := cl.ShardsInBucket("n0")
	sealed, active := countStates(shards)
	if sealed != 1 || active != 1 {
		t.Fatalf("sealed=%d active=%d", sealed, active)
	}

	cl.DriveVolumeSeal(key)
	cl.ExpectUnavailable(key)
}
