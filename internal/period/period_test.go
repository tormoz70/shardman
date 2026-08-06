package period

import (
	"encoding/json"
	"testing"
	"time"
)

func TestTimeDayPeriodID(t *testing.T) {
	spec, err := ParseSpec(AxisTime, json.RawMessage(`{"unit":"day"}`))
	if err != nil {
		t.Fatal(err)
	}
	pid, err := spec.ID("2026-08-06T12:00:00Z")
	if err != nil || pid != "2026-08-06" {
		t.Fatalf("got %q err %v", pid, err)
	}
}

func TestNumericPeriodID(t *testing.T) {
	spec, err := ParseSpec(AxisNumeric, json.RawMessage(`{"width":1000000}`))
	if err != nil {
		t.Fatal(err)
	}
	pid, err := spec.ID(int64(2_500_000))
	if err != nil || pid != "p2" {
		t.Fatalf("got %q err %v", pid, err)
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
	if err != nil || r.Kind != RoutePeriod || r.PeriodID != "2026-09" {
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
	if err != nil || r.Kind != RoutePeriod || r.PeriodID != "2026-08" {
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
