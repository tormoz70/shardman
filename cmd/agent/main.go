package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tormoz70/shardman/internal/config"
	"github.com/tormoz70/shardman/internal/fsm"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg, err := config.LoadAgent()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	ticker := time.NewTicker(cfg.StatsInterval)
	defer ticker.Stop()

	if err := tick(ctx, cfg); err != nil {
		slog.Warn("tick", "err", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := tick(ctx, cfg); err != nil {
				slog.Warn("tick", "err", err)
			}
		}
	}
}

func tick(ctx context.Context, cfg config.AgentConfig) error {
	conn, err := pgx.Connect(ctx, cfg.PGDSN)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	var size int64
	if err := conn.QueryRow(ctx, `SELECT pg_database_size(current_database())`).Scan(&size); err != nil {
		return err
	}

	state, err := postStats(ctx, cfg, size)
	if err != nil {
		return err
	}
	if state == string(fsm.StateCleaning) {
		if err := cleanDatabase(ctx, conn); err != nil {
			return err
		}
		return postCleaned(ctx, cfg)
	}
	return nil
}

func postStats(ctx context.Context, cfg config.AgentConfig, size int64) (string, error) {
	body, _ := json.Marshal(map[string]any{
		"shard_uuid":     cfg.ShardUUID,
		"reported_bytes": size,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.CoordinatorURL+"/v1/internal/stats", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.ClusterKey != "" {
		req.Header.Set("X-Cluster-Key", cfg.ClusterKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("stats status %d", resp.StatusCode)
	}
	var out struct {
		State string `json:"state"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out.State, nil
}

func cleanDatabase(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, `
		DO $$ DECLARE r RECORD; BEGIN
			FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename NOT LIKE 'pg_%') LOOP
				EXECUTE 'TRUNCATE TABLE public.' || quote_ident(r.tablename) || ' CASCADE';
			END LOOP;
		END $$;`)
	return err
}

func postCleaned(ctx context.Context, cfg config.AgentConfig) error {
	body, _ := json.Marshal(map[string]string{"shard_uuid": cfg.ShardUUID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.CoordinatorURL+"/v1/internal/cleaned", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Cluster-Key", cfg.ClusterKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("cleaned status %d", resp.StatusCode)
	}
	slog.Info("shard cleaned and recycled")
	return nil
}
