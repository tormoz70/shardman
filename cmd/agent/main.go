package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	shardmanv1 "github.com/tormoz70/shardman/api/gen/shardman/v1"
	"github.com/tormoz70/shardman/internal/config"
	"github.com/tormoz70/shardman/internal/fsm"
	"github.com/tormoz70/shardman/internal/oteltrace"
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

	_, _, dialOpts, err := oteltrace.Setup(ctx)
	if err != nil {
		slog.Error("otel", "err", err)
		os.Exit(1)
	}
	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(cfg.CoordinatorAddr, dialOpts...)
	if err != nil {
		slog.Error("grpc dial", "err", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := shardmanv1.NewInternalServiceClient(conn)

	ticker := time.NewTicker(cfg.StatsInterval)
	defer ticker.Stop()

	if err := tick(ctx, cfg, client); err != nil {
		slog.Warn("tick", "err", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := tick(ctx, cfg, client); err != nil {
				slog.Warn("tick", "err", err)
			}
		}
	}
}

func tick(ctx context.Context, cfg config.AgentConfig, client shardmanv1.InternalServiceClient) error {
	pgConn, err := pgx.Connect(ctx, cfg.PGDSN)
	if err != nil {
		return err
	}
	defer pgConn.Close(ctx)

	size, err := measureSize(ctx, pgConn, cfg.SizeSource)
	if err != nil {
		return err
	}

	rpcCtx := ctx
	if cfg.ClusterKey != "" {
		rpcCtx = metadata.AppendToOutgoingContext(ctx, "x-cluster-key", cfg.ClusterKey)
	}
	resp, err := client.ReportStats(rpcCtx, &shardmanv1.ReportStatsRequest{
		ShardUuid:     cfg.ShardUUID,
		ReportedBytes: size,
	})
	if err != nil {
		return err
	}

	switch resp.GetState() {
	case string(fsm.StateDraining):
		if err := drainShard(ctx, pgConn, cfg); err != nil {
			return err
		}
		_, err = client.ReportDrainComplete(rpcCtx, &shardmanv1.ReportDrainCompleteRequest{ShardUuid: cfg.ShardUUID})
		return err
	case string(fsm.StateCleaning):
		if err := cleanDatabase(ctx, pgConn); err != nil {
			return err
		}
		_, err = client.ReportCleaned(rpcCtx, &shardmanv1.ReportCleanedRequest{ShardUuid: cfg.ShardUUID})
		if err == nil {
			slog.Info("shard cleaned and recycled")
		}
		return err
	}
	return nil
}

func measureSize(ctx context.Context, conn *pgx.Conn, source string) (int64, error) {
	var size int64
	if source == "relations" {
		err := conn.QueryRow(ctx, `
			SELECT COALESCE(SUM(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename))), 0)::bigint
			FROM pg_tables WHERE schemaname = 'public'`).Scan(&size)
		return size, err
	}
	err := conn.QueryRow(ctx, `SELECT pg_database_size(current_database())`).Scan(&size)
	return size, err
}

func drainShard(ctx context.Context, conn *pgx.Conn, cfg config.AgentConfig) error {
	if cfg.DrainMode == "terminate" {
		_, err := conn.Exec(ctx, `
			SELECT pg_terminate_backend(pid) FROM pg_stat_activity
			WHERE datname = current_database() AND pid <> pg_backend_pid()`)
		return err
	}
	if cfg.AppDBRole != "" {
		_, err := conn.Exec(ctx, fmt.Sprintf(`REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public FROM %s`, quoteIdent(cfg.AppDBRole)))
		return err
	}
	slog.Info("drain: no APP_DB_ROLE; skipping revoke")
	return nil
}

func quoteIdent(s string) string {
	return `"` + s + `"`
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
