package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	base := flag.String("url", getenv("SHARDMAN_URL", "http://localhost:8080"), "shardman server URL")
	key := flag.String("key", os.Getenv("CLUSTER_KEY"), "cluster key")
	flag.Parse()

	if flag.NArg() < 1 {
		printUsage()
		os.Exit(1)
	}

	switch flag.Arg(0) {
	case "bootstrap":
		runBootstrap(*base, *key, flag.Args()[1:])
	case "shards":
		runShards(*base, *key)
	case "register":
		runRegister(*base, *key, flag.Args()[1:])
	case "resolve-write":
		runResolve(*base, "/v1/resolve/write", flag.Args()[1:])
	case "resolve-read":
		runResolve(*base, "/v1/resolve/read", flag.Args()[1:])
	case "seal-rotate":
		runSealRotate(*base, *key, flag.Args()[1:])
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage:
  shardman bootstrap --axis time --unit month --retention 3 --future 1 --max-bytes 1073741824
  shardman shards
  shardman register --uuid <uuid> --dsn <dsn> [--role error|data] [--url advertise]
  shardman resolve-write --key "2026-08-06T00:00:00Z"
  shardman resolve-read --key "2026-08-06T00:00:00Z"
  shardman seal-rotate --period 2026-08
`)
}

func runBootstrap(base, key string, args []string) {
	fs := flag.NewFlagSet("bootstrap", flag.ExitOnError)
	axis := fs.String("axis", "time", "period axis")
	unit := fs.String("unit", "month", "time unit")
	width := fs.Int64("width", 0, "numeric width")
	retention := fs.Int("retention", 3, "retention depth")
	future := fs.Int("future", 0, "max future periods")
	maxBytes := fs.Int64("max-bytes", 1<<30, "shard max bytes")
	_ = fs.Parse(args)

	var spec json.RawMessage
	if *axis == "numeric" {
		spec, _ = json.Marshal(map[string]int64{"width": *width})
	} else {
		spec, _ = json.Marshal(map[string]string{"unit": *unit})
	}
	body, _ := json.Marshal(map[string]any{
		"mode":               "range",
		"period_axis":        *axis,
		"period_spec":        spec,
		"shard_max_bytes":    *maxBytes,
		"retention_depth":    *retention,
		"max_future_periods": *future,
		"cluster_key":        key,
	})
	doPOST(base+"/v1/admin/bootstrap", key, body)
}

func runShards(base, key string) {
	doGET(base+"/v1/admin/shards", key)
}

func runRegister(base, key string, args []string) {
	fs := flag.NewFlagSet("register", flag.ExitOnError)
	uuid := fs.String("uuid", "", "shard uuid")
	dsn := fs.String("dsn", "", "postgres dsn")
	role := fs.String("role", "data", "data|error")
	url := fs.String("url", "", "advertise url")
	_ = fs.Parse(args)
	body, _ := json.Marshal(map[string]string{
		"shard_uuid":    *uuid,
		"dsn":           *dsn,
		"role":          *role,
		"advertise_url": *url,
		"startup_state": "standby",
		"cluster_key":   key,
	})
	doPOST(base+"/v1/admin/shards", key, body)
}

func runResolve(base, path string, args []string) {
	fs := flag.NewFlagSet("resolve", flag.ExitOnError)
	keyVal := fs.String("key", "", "shard key")
	_ = fs.Parse(args)
	body, _ := json.Marshal(map[string]string{"shard_key": *keyVal})
	req, _ := http.NewRequest(http.MethodPost, base+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
	fmt.Println()
}

func runSealRotate(base, key string, args []string) {
	fs := flag.NewFlagSet("seal", flag.ExitOnError)
	period := fs.String("period", "", "period id")
	_ = fs.Parse(args)
	body, _ := json.Marshal(map[string]string{"period_id": *period, "cluster_key": key})
	doPOST(base+"/v1/admin/seal-rotate", key, body)
}

func doPOST(url, key string, body []byte) {
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Cluster-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
	fmt.Println()
}

func doGET(url, key string) {
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-Cluster-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
	fmt.Println()
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
