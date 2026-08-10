//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/tormoz70/shardman/internal/bucket"
	"github.com/tormoz70/shardman/internal/fsm"
)

func TestTimeCurrentBucket(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.BootstrapTime(3, 0, 1024)
	e.RegisterError()
	e.RegisterStandbys(1)

	res, err := e.Resolver.ResolveWrite(e.Ctx, "2026-08-01")
	if err != nil {
		t.Fatal(err)
	}
	if res.Routing != bucket.RouteBucket || res.BucketID != "2026-08" {
		t.Fatalf("got %+v", res)
	}
}

func TestTimeFutureDisallowed(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.BootstrapTime(3, 0, 1024)
	e.RegisterError()
	e.RegisterStandbys(1)

	res, err := e.Resolver.ResolveWrite(e.Ctx, "2026-10-01")
	if err != nil {
		t.Fatal(err)
	}
	if res.Routing != bucket.RouteError || res.Reason != bucket.ReasonFutureDisallowed {
		t.Fatalf("got %+v", res)
	}
}

func TestTimeFutureAllowedAndTooFar(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.BootstrapTime(3, 1, 1024)
	e.RegisterError()
	e.RegisterStandbys(2)

	res, err := e.Resolver.ResolveWrite(e.Ctx, "2026-09-01")
	if err != nil {
		t.Fatal(err)
	}
	if res.Routing != bucket.RouteBucket || res.BucketID != "2026-09" {
		t.Fatalf("future+1: %+v", res)
	}

	res, err = e.Resolver.ResolveWrite(e.Ctx, "2026-10-01")
	if err != nil {
		t.Fatal(err)
	}
	if res.Routing != bucket.RouteError || res.Reason != bucket.ReasonFutureTooFar {
		t.Fatalf("future too far: %+v", res)
	}
}

func TestTimeEvictedWriteAndRead(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.BootstrapTime(3, 0, 1024)
	e.RegisterError()
	e.RegisterStandbys(1)

	res, err := e.Resolver.ResolveWrite(e.Ctx, "2026-04-01")
	if err != nil {
		t.Fatal(err)
	}
	if res.Routing != bucket.RouteError || res.Reason != bucket.ReasonEvicted {
		t.Fatalf("write: %+v", res)
	}

	read, err := e.Resolver.ResolveRead(e.Ctx, "2026-04-01")
	if err != nil {
		t.Fatal(err)
	}
	if read.Routing != bucket.RouteError || read.Reason != bucket.ReasonEvicted || len(read.Shards) != 0 {
		t.Fatalf("read: %+v", read)
	}
}

func TestTimeRetentionClean(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.SetNow(time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC))
	e.BootstrapTime(3, 0, 1024)
	e.RegisterError()
	e.RegisterStandbys(2)

	res, err := e.Resolver.ResolveWrite(e.Ctx, "2026-04-01")
	if err != nil {
		t.Fatal(err)
	}
	if res.BucketID != "2026-04" {
		t.Fatalf("bucket %s", res.BucketID)
	}
	oldUUID, err := uuid.Parse(res.UUID)
	if err != nil {
		t.Fatal(err)
	}

	e.SetNow(time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC))
	if err := e.Retention.Tick(e.Ctx); err != nil {
		t.Fatal(err)
	}

	sh, err := e.Store.GetShardByUUID(e.Ctx, oldUUID)
	if err != nil {
		t.Fatal(err)
	}
	if sh.State != fsm.StateCleaning {
		t.Fatalf("expected cleaning, got %s", sh.State)
	}

	if err := e.Store.FinishCleaning(e.Ctx, oldUUID); err != nil {
		t.Fatal(err)
	}
	sh, err = e.Store.GetShardByUUID(e.Ctx, oldUUID)
	if err != nil {
		t.Fatal(err)
	}
	if sh.State != fsm.StateStandby || sh.BucketID != nil {
		t.Fatalf("after clean: state=%s bucket=%v", sh.State, sh.BucketID)
	}
}

func TestTimeErrorShardNotCleaned(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.SetNow(time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC))
	e.BootstrapTime(3, 0, 1024)
	errUUID := e.RegisterError()
	e.RegisterStandbys(1)

	_, err := e.Resolver.ResolveWrite(e.Ctx, "2026-04-01")
	if err != nil {
		t.Fatal(err)
	}

	e.SetNow(time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC))
	if err := e.Retention.Tick(e.Ctx); err != nil {
		t.Fatal(err)
	}

	sh, err := e.Store.GetShardByUUID(e.Ctx, errUUID)
	if err != nil {
		t.Fatal(err)
	}
	if sh.State != fsm.StateActive || sh.Role != fsm.RoleError {
		t.Fatalf("error shard changed: %+v", sh)
	}
}
