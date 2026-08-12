package agent

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/metadata"

	shardmanv1 "github.com/tormoz70/shardman/api/gen/shardman/v1"
	"github.com/tormoz70/shardman/internal/config"
	"github.com/tormoz70/shardman/internal/fsm"
)

// Tick measures shard size, reports stats, and handles drain/clean FSM actions.
func Tick(ctx context.Context, cfg config.AgentConfig, client shardmanv1.InternalServiceClient) error {
	pgConn, err := pgx.Connect(ctx, cfg.PGDSN)
	if err != nil {
		return err
	}
	defer pgConn.Close(ctx)

	size, err := MeasureSize(ctx, pgConn, cfg.SizeSource)
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
		if err := DrainShard(ctx, pgConn, cfg); err != nil {
			return err
		}
		_, err = client.ReportDrainComplete(rpcCtx, &shardmanv1.ReportDrainCompleteRequest{ShardUuid: cfg.ShardUUID})
		return err
	case string(fsm.StateCleaning):
		if err := CleanDatabase(ctx, pgConn); err != nil {
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

func MeasureSize(ctx context.Context, conn *pgx.Conn, source string) (int64, error) {
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

func DrainShard(ctx context.Context, conn *pgx.Conn, cfg config.AgentConfig) error {
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

func CleanDatabase(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, `
		DO $$ DECLARE r RECORD; BEGIN
			FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename NOT LIKE 'pg_%') LOOP
				EXECUTE 'TRUNCATE TABLE public.' || quote_ident(r.tablename) || ' CASCADE';
			END LOOP;
		END $$;`)
	return err
}
