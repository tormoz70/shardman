package bucket

import (
	"encoding/json"
	"testing"
	"time"
)

func TestTimeDayBucketID(t *testing.T) {
	spec, err := ParseSpec(AxisTime, json.RawMessage(`{"unit":"day"}`))
	if err != nil {
		t.Fatal(err)
	}
	bid, err := spec.ID("2026-08-06T12:00:00Z")
	if err != nil || bid != "2026-08-06" {
		t.Fatalf("got %q err %v", bid, err)
	}
}

func TestNumericBucketID(t *testing.T) {
	spec, err := ParseSpec(AxisNumeric, json.RawMessage(`{"width":1000000}`))
	if err != nil {
		t.Fatal(err)
	}
	bid, err := spec.ID(int64(2_500_000))
	if err != nil || bid != "n2" {
		t.Fatalf("got %q err %v", bid, err)
	}
}

func TestHashBucketIDStable(t *testing.T) {
	spec, err := ParseSpec(AxisHash, json.RawMessage(`{"bucket_count":256,"hash_algo":"xxhash64"}`))
	if err != nil {
		t.Fatal(err)
	}
	a, err := spec.ID("user-42")
	if err != nil {
		t.Fatal(err)
	}
	b, err := spec.ID("user-42")
	if err != nil || a != b {
		t.Fatalf("unstable %q vs %q", a, b)
	}
	if len(a) < 2 || a[0] != 'h' {
		t.Fatalf("unexpected id %q", a)
	}
	c, err := spec.ID("user-43")
	if err != nil {
		t.Fatal(err)
	}
	// different keys usually land in different buckets; not guaranteed — just ensure valid format
	if c[0] != 'h' {
		t.Fatalf("unexpected id %q", c)
	}
}

func TestHashDefaultAlgo(t *testing.T) {
	spec, err := ParseSpec(AxisHash, json.RawMessage(`{"bucket_count":16}`))
	if err != nil {
		t.Fatal(err)
	}
	if spec.HashAlgo != HashAlgoXXHash64 {
		t.Fatalf("algo %q", spec.HashAlgo)
	}
}

func TestClassifyWriteFutureAndEvicted(t *testing.T) {
	spec, _ := ParseSpec(AxisTime, json.RawMessage(`{"unit":"month"}`))
	now := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)

	r, err := ClassifyWrite(spec, now, "2026-10-01", 3, 0)
	if err != nil || r.Kind != RouteError || r.Reason != ReasonFutureDisallowed {
		t.Fatalf("future F=0: %+v %v", r, err)
	}

	r, err = ClassifyWrite(spec, now, "2026-09-01", 3, 1)
	if err != nil || r.Kind != RouteBucket || r.BucketID != "2026-09" {
		t.Fatalf("future F=1: %+v", r)
	}

	r, err = ClassifyWrite(spec, now, "2026-10-01", 3, 1)
	if err != nil || r.Kind != RouteError || r.Reason != ReasonFutureTooFar {
		t.Fatalf("future too far: %+v", r)
	}

	r, err = ClassifyWrite(spec, now, "2026-04-01", 3, 0)
	if err != nil || r.Kind != RouteError || r.Reason != ReasonEvicted {
		t.Fatalf("evicted: %+v", r)
	}

	r, err = ClassifyWrite(spec, now, "2026-08-01", 3, 0)
	if err != nil || r.Kind != RouteBucket || r.BucketID != "2026-08" {
		t.Fatalf("current: %+v", r)
	}
}

func TestMinShards(t *testing.T) {
	if got := MinShards(3, 1); got != 6 {
		t.Fatalf("got %d", got)
	}
	if got := MinShards(3, 0); got != 5 {
		t.Fatalf("got %d", got)
	}
}

func TestParseTimeKeyMillisecondBefore2001(t *testing.T) {
	spec, err := ParseSpec(AxisTime, json.RawMessage(`{"unit":"day"}`))
	if err != nil {
		t.Fatal(err)
	}
	// 946684800000 ms = 2000-01-01 UTC; must not be parsed as seconds.
	bid, err := spec.ID(int64(946684800000))
	if err != nil {
		t.Fatal(err)
	}
	if bid != "2000-01-01" {
		t.Fatalf("got bucket %q, want 2000-01-01", bid)
	}
}

func TestParseTimeKeySecondsStillWork(t *testing.T) {
	spec, err := ParseSpec(AxisTime, json.RawMessage(`{"unit":"day"}`))
	if err != nil {
		t.Fatal(err)
	}
	// 946684800 sec = 2000-01-01 UTC.
	bid, err := spec.ID(int64(946684800))
	if err != nil {
		t.Fatal(err)
	}
	if bid != "2000-01-01" {
		t.Fatalf("got bucket %q for sec timestamp", bid)
	}
}

func TestHashBucketIDCaseInsensitive(t *testing.T) {
	spec, err := ParseSpec(AxisHash, json.RawMessage(`{"bucket_count":256,"hash_algo":"xxhash64"}`))
	if err != nil {
		t.Fatal(err)
	}
	upper, err := spec.ID("ABC-UUID")
	if err != nil {
		t.Fatal(err)
	}
	lower, err := spec.ID("abc-uuid")
	if err != nil {
		t.Fatal(err)
	}
	if upper != lower {
		t.Fatalf("case mismatch: %q vs %q", upper, lower)
	}
}

func TestValidateBootstrapHash(t *testing.T) {
	if err := ValidateBootstrap(ModeHash, AxisHash, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := ValidateBootstrap(ModeRange, AxisHash, nil, nil); err == nil {
		t.Fatal("expected error")
	}
}
