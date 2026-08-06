package period

import (
	"testing"
	"time"
)

func TestPeriodsOutsideRetention(t *testing.T) {
	spec, _ := ParseSpec(AxisTime, []byte(`{"unit":"month"}`))
	now := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	out, err := PeriodsOutsideRetention(spec, now, 3, []string{"2026-04", "2026-06", "2026-08"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0] != "2026-04" {
		t.Fatalf("got %v", out)
	}
}
