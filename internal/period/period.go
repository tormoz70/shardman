package period

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const ErrorPeriodID = "__error__"

type Axis string

const (
	AxisTime    Axis = "time"
	AxisNumeric Axis = "numeric"
)

type TimeUnit string

const (
	UnitHour  TimeUnit = "hour"
	UnitDay   TimeUnit = "day"
	UnitMonth TimeUnit = "month"
)

type Spec struct {
	Axis      Axis            `json:"axis"`
	TimeUnit  TimeUnit        `json:"unit,omitempty"`
	Width     int64           `json:"width,omitempty"`
	Raw       json.RawMessage `json:"-"`
}

func ParseSpec(axis Axis, raw json.RawMessage) (Spec, error) {
	s := Spec{Axis: axis, Raw: raw}
	if len(raw) == 0 {
		return s, fmt.Errorf("period_spec required")
	}
	switch axis {
	case AxisTime:
		var body struct {
			Unit TimeUnit `json:"unit"`
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			return s, err
		}
		switch body.Unit {
		case UnitHour, UnitDay, UnitMonth:
			s.TimeUnit = body.Unit
		default:
			return s, fmt.Errorf("unsupported time unit %q", body.Unit)
		}
	case AxisNumeric:
		var body struct {
			Width int64 `json:"width"`
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			return s, err
		}
		if body.Width <= 0 {
			return s, fmt.Errorf("width must be positive")
		}
		s.Width = body.Width
	default:
		return s, fmt.Errorf("unsupported axis %q", axis)
	}
	return s, nil
}

// ID returns stable period_id string for a shard key (time or numeric).
func (s Spec) ID(shardKey any) (string, error) {
	switch s.Axis {
	case AxisTime:
		t, err := parseTimeKey(shardKey)
		if err != nil {
			return "", err
		}
		return s.timeID(t.UTC()), nil
	case AxisNumeric:
		n, err := parseNumericKey(shardKey)
		if err != nil {
			return "", err
		}
		idx := n / s.Width
		if n < 0 {
			idx = (n - s.Width + 1) / s.Width
		}
		return fmt.Sprintf("p%d", idx), nil
	default:
		return "", fmt.Errorf("unsupported axis")
	}
}

func (s Spec) CurrentID(now time.Time) (string, error) {
	switch s.Axis {
	case AxisTime:
		return s.timeID(now.UTC()), nil
	case AxisNumeric:
		return "", fmt.Errorf("numeric axis has no wall-clock current period")
	default:
		return "", fmt.Errorf("unsupported axis")
	}
}

func (s Spec) timeID(t time.Time) string {
	switch s.TimeUnit {
	case UnitHour:
		return t.Format("2006-01-02T15")
	case UnitDay:
		return t.Format("2006-01-02")
	case UnitMonth:
		return t.Format("2006-01")
	default:
		return t.Format("2006-01-02")
	}
}

// Index maps period_id to comparable int64 (time axis only).
func (s Spec) Index(periodID string) (int64, error) {
	if s.Axis != AxisTime {
		return 0, fmt.Errorf("index only for time axis")
	}
	switch s.TimeUnit {
	case UnitHour:
		t, err := time.Parse("2006-01-02T15", periodID)
		if err != nil {
			return 0, err
		}
		return t.Unix() / 3600, nil
	case UnitDay:
		t, err := time.Parse("2006-01-02", periodID)
		if err != nil {
			return 0, err
		}
		return t.Unix() / 86400, nil
	case UnitMonth:
		parts := strings.Split(periodID, "-")
		if len(parts) != 2 {
			return 0, fmt.Errorf("invalid month period_id")
		}
		y, _ := strconv.Atoi(parts[0])
		m, _ := strconv.Atoi(parts[1])
		return int64(y*12 + m - 1), nil
	default:
		return 0, fmt.Errorf("unsupported unit")
	}
}

func (s Spec) IDFromIndex(idx int64) (string, error) {
	if s.Axis != AxisTime {
		return "", fmt.Errorf("index only for time axis")
	}
	switch s.TimeUnit {
	case UnitHour:
		t := time.Unix(idx*3600, 0).UTC()
		return t.Format("2006-01-02T15"), nil
	case UnitDay:
		t := time.Unix(idx*86400, 0).UTC()
		return t.Format("2006-01-02"), nil
	case UnitMonth:
		y := idx / 12
		m := idx%12 + 1
		return fmt.Sprintf("%04d-%02d", y, m), nil
	default:
		return "", fmt.Errorf("unsupported unit")
	}
}

// MinShards returns required physical shard count for time axis deploy.
func MinShards(retentionDepth, maxFuture int) int {
	return retentionDepth + 1 + maxFuture + 1
}

func parseTimeKey(shardKey any) (time.Time, error) {
	switch v := shardKey.(type) {
	case string:
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t, nil
		}
		if t, err := time.Parse("2006-01-02", v); err == nil {
			return t, nil
		}
		if t, err := time.Parse("2006-01", v); err == nil {
			return t, nil
		}
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
			if ms > 1_000_000_000_000 {
				return time.UnixMilli(ms), nil
			}
			return time.Unix(ms, 0), nil
		}
		return time.Time{}, fmt.Errorf("invalid time shard_key")
	case float64:
		if v > 1_000_000_000_000 {
			return time.UnixMilli(int64(v)), nil
		}
		return time.Unix(int64(v), 0), nil
	case int64:
		if v > 1_000_000_000_000 {
			return time.UnixMilli(v), nil
		}
		return time.Unix(v, 0), nil
	case int:
		return parseTimeKey(int64(v))
	default:
		return time.Time{}, fmt.Errorf("unsupported shard_key type")
	}
}

func parseNumericKey(shardKey any) (int64, error) {
	switch v := shardKey.(type) {
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported numeric shard_key")
	}
}
