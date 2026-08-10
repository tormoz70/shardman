package client

import (
	"testing"

	shardmanv1 "github.com/tormoz70/shardman/api/gen/shardman/v1"
	"github.com/tormoz70/shardman/internal/bucket"
	"github.com/tormoz70/shardman/internal/fsm"
)

func TestTryResolveWriteLocal_Hash(t *testing.T) {
	spec := JSONSpec(map[string]any{"bucket_count": 4, "hash_algo": bucket.HashAlgoXXHash64})
	parsed, err := bucket.ParseSpec(bucket.AxisHash, spec)
	if err != nil {
		t.Fatal(err)
	}
	bucketID, err := parsed.ID("user-42")
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{
		topo: &shardmanv1.Topology{
			TopologyVersion: 1,
			Config: &shardmanv1.ClusterConfig{
				Mode:           bucket.ModeHash,
				BucketAxis:     string(bucket.AxisHash),
				BucketSpecJson: spec,
				ShardMaxBytes:  1_000_000,
			},
			Shards: []*shardmanv1.Shard{
				{
					Id:        10,
					ShardUuid: "11111111-1111-1111-1111-111111111111",
					Role:      string(fsm.RoleData),
					BucketId:  bucketID,
					State:     string(fsm.StateActive),
					Dsn:       "postgres://active/db",
				},
			},
		},
	}

	res, ok := c.tryResolveWriteLocal("user-42")
	if !ok {
		t.Fatal("expected local hit")
	}
	if res.Endpoint != "postgres://active/db" {
		t.Fatalf("endpoint=%q", res.Endpoint)
	}
	if res.Routing != bucket.RouteBucket {
		t.Fatalf("routing=%s", res.Routing)
	}
}

func TestTryResolveWriteLocal_MissWithoutActive(t *testing.T) {
	spec := JSONSpec(map[string]any{"bucket_count": 4, "hash_algo": bucket.HashAlgoXXHash64})
	c := &Client{
		topo: &shardmanv1.Topology{
			Config: &shardmanv1.ClusterConfig{
				Mode:           bucket.ModeHash,
				BucketAxis:     string(bucket.AxisHash),
				BucketSpecJson: spec,
			},
			Shards: []*shardmanv1.Shard{},
		},
	}
	_, ok := c.tryResolveWriteLocal("user-42")
	if ok {
		t.Fatal("expected miss without active shard")
	}
}

func TestTryResolveReadLocal_ActiveAndSealed(t *testing.T) {
	spec := JSONSpec(map[string]int64{"width": 1000})
	c := &Client{
		topo: &shardmanv1.Topology{
			Config: &shardmanv1.ClusterConfig{
				Mode:           bucket.ModeRange,
				BucketAxis:     string(bucket.AxisNumeric),
				BucketSpecJson: spec,
			},
			Shards: []*shardmanv1.Shard{
				{Id: 1, Role: string(fsm.RoleData), BucketId: "n0", State: string(fsm.StateSealed), Dsn: "postgres://sealed/db"},
				{Id: 2, Role: string(fsm.RoleData), BucketId: "n0", State: string(fsm.StateActive), Dsn: "postgres://active/db"},
			},
		},
	}
	res, ok := c.tryResolveReadLocal(int64(500))
	if !ok {
		t.Fatal("expected local read hit")
	}
	if len(res.Shards) != 2 {
		t.Fatalf("shards=%d", len(res.Shards))
	}
}
