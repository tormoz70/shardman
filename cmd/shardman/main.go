package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	shardmanv1 "github.com/tormoz70/shardman/api/gen/shardman/v1"
	"github.com/tormoz70/shardman/pkg/client"
)

func main() {
	addr := flag.String("addr", getenv("SHARDMAN_ADDR", "localhost:9090"), "shardman gRPC address")
	key := flag.String("key", os.Getenv("CLUSTER_KEY"), "cluster key")
	flag.Parse()

	if flag.NArg() < 1 {
		printUsage()
		os.Exit(1)
	}

	ctx := context.Background()
	c, err := client.Dial(ctx, *addr, client.Options{ClusterKey: *key})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer c.Close()

	switch flag.Arg(0) {
	case "bootstrap":
		runBootstrap(ctx, c, flag.Args()[1:])
	case "shards":
		runShards(ctx, c)
	case "register":
		runRegister(ctx, c, flag.Args()[1:])
	case "resolve-write":
		runResolveWrite(ctx, c, flag.Args()[1:])
	case "resolve-read":
		runResolveRead(ctx, c, flag.Args()[1:])
	case "seal-rotate":
		runSealRotate(ctx, c, flag.Args()[1:])
	case "topology":
		runTopology(ctx, c)
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage:
  shardman bootstrap --axis time --unit month --retention 3 --future 1 --max-bytes 1073741824
  shardman bootstrap --axis hash --buckets 256 --max-bytes 1073741824
  shardman shards
  shardman topology
  shardman register --uuid <uuid> --dsn <dsn> [--role error|data] [--url advertise]
  shardman resolve-write --key "2026-08-06T00:00:00Z"
  shardman resolve-read --key "2026-08-06T00:00:00Z"
  shardman seal-rotate --bucket 2026-08
`)
}

func runBootstrap(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("bootstrap", flag.ExitOnError)
	axis := fs.String("axis", "time", "bucket axis: time|numeric|hash")
	unit := fs.String("unit", "month", "time unit")
	width := fs.Int64("width", 0, "numeric width")
	buckets := fs.Int("buckets", 256, "hash bucket_count")
	retention := fs.Int("retention", 3, "retention depth (time)")
	future := fs.Int("future", 0, "max future buckets (time)")
	maxBytes := fs.Int64("max-bytes", 1<<30, "shard max bytes")
	_ = fs.Parse(args)

	mode := "range"
	var spec any
	switch *axis {
	case "numeric":
		spec = map[string]int64{"width": *width}
	case "hash":
		mode = "hash"
		spec = map[string]any{"bucket_count": *buckets, "hash_algo": "xxhash64"}
	default:
		spec = map[string]string{"unit": *unit}
	}
	req := &shardmanv1.BootstrapRequest{
		Mode:           mode,
		BucketAxis:     *axis,
		BucketSpecJson: client.JSONSpec(spec),
		ShardMaxBytes:  *maxBytes,
	}
	if *axis == "time" {
		rd := int32(*retention)
		mf := int32(*future)
		req.RetentionDepth = &rd
		req.MaxFutureBuckets = &mf
	}
	resp, err := c.Bootstrap(ctx, req)
	printResult(resp, err)
}

func runShards(ctx context.Context, c *client.Client) {
	topo, err := c.GetTopology(ctx)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	enc, _ := json.MarshalIndent(topo.Shards, "", "  ")
	fmt.Println(string(enc))
}

func runTopology(ctx context.Context, c *client.Client) {
	topo, err := c.GetTopology(ctx)
	printResult(topo, err)
}

func runRegister(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("register", flag.ExitOnError)
	u := fs.String("uuid", "", "shard uuid")
	dsn := fs.String("dsn", "", "postgres dsn")
	role := fs.String("role", "data", "data|error")
	url := fs.String("url", "", "advertise url")
	_ = fs.Parse(args)
	resp, err := c.RegisterShard(ctx, &shardmanv1.RegisterShardRequest{
		ShardUuid:    *u,
		Dsn:          *dsn,
		Role:         *role,
		AdvertiseUrl: *url,
		StartupState: "standby",
	})
	printResult(resp, err)
}

func runResolveWrite(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("resolve-write", flag.ExitOnError)
	keyVal := fs.String("key", "", "shard key")
	_ = fs.Parse(args)
	res, err := c.ResolveWrite(ctx, *keyVal)
	printResult(res, err)
}

func runResolveRead(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("resolve-read", flag.ExitOnError)
	keyVal := fs.String("key", "", "shard key")
	_ = fs.Parse(args)
	res, err := c.ResolveRead(ctx, *keyVal)
	printResult(res, err)
}

func runSealRotate(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("seal", flag.ExitOnError)
	bucketID := fs.String("bucket", "", "bucket id")
	_ = fs.Parse(args)
	err := c.SealRotate(ctx, *bucketID)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println(`{"status":"rotated"}`)
}

func printResult(v any, err error) {
	if err != nil {
		if st, ok := status.FromError(err); ok {
			fmt.Printf("error: %s (%s)\n", st.Message(), codes.Code(st.Code()))
		} else {
			fmt.Println(err)
		}
		os.Exit(1)
	}
	enc, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(enc))
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
