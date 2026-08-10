package bucket

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
)

const ErrorBucketID = "__error__"

const (
	ModeRange = "range"
	ModeHash  = "hash"
)

type Axis string

const (
	AxisTime    Axis = "time"
	AxisNumeric Axis = "numeric"
	AxisHash    Axis = "hash"
)

type TimeUnit string

const (
	UnitHour  TimeUnit = "hour"
	UnitDay   TimeUnit = "day"
	UnitMonth TimeUnit = "month"
)

const HashAlgoXXHash64 = "xxhash64"

// unixSecondsYear3000 is the Unix timestamp for year ~3000 in seconds.
// Values above this threshold are treated as milliseconds in parseTimeKey.
const unixSecondsYear3000 = 32_536_800_000

type Spec struct {
	Axis        Axis            `json:"axis"`
	TimeUnit    TimeUnit        `json:"unit,omitempty"`
	Width       int64           `json:"width,omitempty"`
	BucketCount int             `json:"bucket_count,omitempty"`
	HashAlgo    string          `json:"hash_algo,omitempty"`
	Raw         json.RawMessage `json:"-"`
}

func ParseSpec(axis Axis, raw json.RawMessage) (Spec, error) {
	s := Spec{Axis: axis, Raw: raw}
	if len(raw) == 0 {
		return s, fmt.Errorf("bucket_spec required")
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
	case AxisHash:
		var body struct {
			BucketCount int    `json:"bucket_count"`
			HashAlgo    string `json:"hash_algo"`
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			return s, err
		}
		if body.BucketCount <= 0 {
			return s, fmt.Errorf("bucket_count must be positive")
		}
		if body.HashAlgo == "" {
			body.HashAlgo = HashAlgoXXHash64
		}
		if body.HashAlgo != HashAlgoXXHash64 {
			return s, fmt.Errorf("unsupported hash_algo %q", body.HashAlgo)
		}
		s.BucketCount = body.BucketCount
		s.HashAlgo = body.HashAlgo
	default:
		return s, fmt.Errorf("unsupported axis %q", axis)
	}
	return s, nil
}

// ID returns stable bucket_id string for a shard key.
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
		return fmt.Sprintf("n%d", idx), nil
	case AxisHash:
		h, err := hashKey(shardKey, s.HashAlgo)
		if err != nil {
			return "", err
		}
		idx := int(h % uint64(s.BucketCount))
		width := digitWidth(s.BucketCount - 1)
		return fmt.Sprintf("h%0*d", width, idx), nil
	default:
		return "", fmt.Errorf("unsupported axis")
	}
}

func (s Spec) CurrentID(now time.Time) (string, error) {
	switch s.Axis {
	case AxisTime:
		return s.timeID(now.UTC()), nil
	case AxisNumeric, AxisHash:
		return "", fmt.Errorf("%s axis has no wall-clock current bucket", s.Axis)
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

// Index maps bucket_id to comparable int64 (time axis only).
func (s Spec) Index(bucketID string) (int64, error) {
	if s.Axis != AxisTime {
		return 0, fmt.Errorf("index only for time axis")
	}
	switch s.TimeUnit {
	case UnitHour:
		t, err := time.Parse("2006-01-02T15", bucketID)
		if err != nil {
			return 0, err
		}
		return t.Unix() / 3600, nil
	case UnitDay:
		t, err := time.Parse("2006-01-02", bucketID)
		if err != nil {
			return 0, err
		}
		return t.Unix() / 86400, nil
	case UnitMonth:
		parts := strings.Split(bucketID, "-")
		if len(parts) != 2 {
			return 0, fmt.Errorf("invalid month bucket_id")
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

func digitWidth(n int) int {
	if n < 10 {
		return 1
	}
	w := 0
	for n > 0 {
		n /= 10
		w++
	}
	return w
}

func hashKey(shardKey any, algo string) (uint64, error) {
	if algo != HashAlgoXXHash64 {
		return 0, fmt.Errorf("unsupported hash_algo %q", algo)
	}
	s, err := stringifyKey(shardKey)
	if err != nil {
		return 0, err
	}
	return xxhash.Sum64String(s), nil
}

func stringifyKey(shardKey any) (string, error) {
	switch v := shardKey.(type) {
	case string:
		return strings.ToLower(v), nil
	case float64:
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10), nil
		}
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case int:
		return strconv.Itoa(v), nil
	case json.Number:
		return v.String(), nil
	default:
		return "", fmt.Errorf("unsupported shard_key type")
	}
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
			if ms > unixSecondsYear3000 {
				return time.UnixMilli(ms), nil
			}
			return time.Unix(ms, 0), nil
		}
		return time.Time{}, fmt.Errorf("invalid time shard_key")
	case float64:
		if v > unixSecondsYear3000 {
			return time.UnixMilli(int64(v)), nil
		}
		return time.Unix(int64(v), 0), nil
	case int64:
		if v > unixSecondsYear3000 {
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
