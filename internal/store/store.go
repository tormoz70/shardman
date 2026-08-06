package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tormoz70/shardman/internal/fsm"
	"github.com/tormoz70/shardman/internal/period"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

var (
	ErrAlreadyBootstrapped = errors.New("cluster already bootstrapped")
	ErrNotBootstrapped     = errors.New("cluster not bootstrapped")
	ErrConflict            = errors.New("conflict")
	ErrNotFound            = errors.New("not found")
)

type ClusterConfig struct {
	Mode             string
	PeriodAxis       period.Axis
	PeriodSpec       period.Spec
	PeriodSpecRaw    json.RawMessage
	ShardMaxBytes    int64
	RetentionDepth   *int
	MaxFuturePeriods *int
}

type Shard struct {
	ID             int64
	ShardUUID      uuid.UUID
	Role           fsm.Role
	PeriodID       *string
	State          fsm.State
	DSN            string
	AdvertiseURL   *string
	ReportedBytes  int64
	MaxBytes       *int64
	LastSeenAt     *time.Time
	SealedAt       *time.Time
	Version        int64
}

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	s := &Store{pool: pool}
	if err := s.Migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) Migrate(ctx context.Context) error {
	sql, err := migrationsFS.ReadFile("migrations/001_init.sql")
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, string(sql))
	return err
}

func (s *Store) Bootstrap(ctx context.Context, cfg ClusterConfig) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM cluster_config)`).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return ErrAlreadyBootstrapped
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO cluster_config (mode, period_axis, period_spec, shard_max_bytes, retention_depth, max_future_periods)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		cfg.Mode, cfg.PeriodAxis, cfg.PeriodSpecRaw, cfg.ShardMaxBytes, cfg.RetentionDepth, cfg.MaxFuturePeriods,
	)
	return tx.Commit(ctx)
}

func (s *Store) GetConfig(ctx context.Context) (*ClusterConfig, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT mode, period_axis, period_spec, shard_max_bytes, retention_depth, max_future_periods
		FROM cluster_config WHERE id = 1`)

	var cfg ClusterConfig
	var raw []byte
	var axis string
	if err := row.Scan(&cfg.Mode, &axis, &raw, &cfg.ShardMaxBytes, &cfg.RetentionDepth, &cfg.MaxFuturePeriods); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotBootstrapped
		}
		return nil, err
	}
	cfg.PeriodAxis = period.Axis(axis)
	cfg.PeriodSpecRaw = raw
	spec, err := period.ParseSpec(cfg.PeriodAxis, raw)
	if err != nil {
		return nil, err
	}
	cfg.PeriodSpec = spec
	return &cfg, nil
}

func scanShard(row pgx.Row) (*Shard, error) {
	var sh Shard
	var role, state string
	err := row.Scan(
		&sh.ID, &sh.ShardUUID, &role, &sh.PeriodID, &state,
		&sh.DSN, &sh.AdvertiseURL, &sh.ReportedBytes, &sh.MaxBytes,
		&sh.LastSeenAt, &sh.SealedAt, &sh.Version,
	)
	if err != nil {
		return nil, err
	}
	sh.Role = fsm.Role(role)
	sh.State = fsm.State(state)
	return &sh, nil
}

const shardCols = `id, shard_uuid, role, period_id, state, dsn, advertise_url, reported_bytes, max_bytes, last_seen_at, sealed_at, version`

func (s *Store) ListShards(ctx context.Context) ([]Shard, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+shardCols+` FROM shards ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Shard
	for rows.Next() {
		sh, err := scanShard(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *sh)
	}
	return out, rows.Err()
}

func (s *Store) RegisterShard(ctx context.Context, shardUUID uuid.UUID, role fsm.Role, dsn, advertiseURL string, startupState fsm.State) (*Shard, error) {
	if role == fsm.RoleError {
		startupState = fsm.StateActive
	} else if startupState == "" {
		startupState = fsm.StateStandby
	}
	if !fsm.StartupStateAllowed(startupState) && role != fsm.RoleError {
		return nil, fmt.Errorf("startup_state must be standby")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if role == fsm.RoleError {
		var cnt int
		if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM shards WHERE role = 'error'`).Scan(&cnt); err != nil {
			return nil, err
		}
		if cnt > 0 {
			return nil, ErrConflict
		}
	}

	var id int64
	err = tx.QueryRow(ctx, `
		INSERT INTO shards (shard_uuid, role, state, dsn, advertise_url)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (shard_uuid) DO UPDATE SET
			dsn = EXCLUDED.dsn,
			advertise_url = EXCLUDED.advertise_url,
			last_seen_at = NOW(),
			updated_at = NOW()
		RETURNING id`,
		shardUUID, role, startupState, dsn, nullStr(advertiseURL),
	).Scan(&id)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return s.GetShardByID(ctx, id)
}

func (s *Store) GetShardByID(ctx context.Context, id int64) (*Shard, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+shardCols+` FROM shards WHERE id = $1`, id)
	sh, err := scanShard(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return sh, err
}

func (s *Store) GetShardByUUID(ctx context.Context, u uuid.UUID) (*Shard, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+shardCols+` FROM shards WHERE shard_uuid = $1`, u)
	sh, err := scanShard(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return sh, err
}

func (s *Store) GetErrorShard(ctx context.Context) (*Shard, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+shardCols+` FROM shards WHERE role = 'error' LIMIT 1`)
	sh, err := scanShard(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return sh, err
}

func (s *Store) ActiveForPeriod(ctx context.Context, periodID string) (*Shard, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+shardCols+` FROM shards
		WHERE role = 'data' AND period_id = $1 AND state = 'active' LIMIT 1`, periodID)
	sh, err := scanShard(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return sh, err
}

func (s *Store) ShardsForPeriodRead(ctx context.Context, periodID string) ([]Shard, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+shardCols+` FROM shards
		WHERE role = 'data' AND period_id = $1 AND state IN ('active', 'sealed')
		ORDER BY id`, periodID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Shard
	for rows.Next() {
		sh, err := scanShard(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *sh)
	}
	return out, rows.Err()
}

func (s *Store) StandbyPool(ctx context.Context) ([]Shard, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+shardCols+` FROM shards
		WHERE role = 'data' AND state = 'standby' AND period_id IS NULL
		ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Shard
	for rows.Next() {
		sh, err := scanShard(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *sh)
	}
	return out, rows.Err()
}

func (s *Store) PromoteStandbyToActive(ctx context.Context, periodID string) (*Shard, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var id int64
	err = tx.QueryRow(ctx, `
		SELECT id FROM shards
		WHERE role = 'data' AND state = 'standby' AND period_id IS NULL
		ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	_, err = tx.Exec(ctx, `
		UPDATE shards SET period_id = $1, state = 'active', updated_at = NOW(), version = version + 1
		WHERE id = $2`, periodID, id)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return s.GetShardByID(ctx, id)
}

func (s *Store) SealRotate(ctx context.Context, periodID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var activeID int64
	var activeVer int64
	err = tx.QueryRow(ctx, `
		SELECT id, version FROM shards
		WHERE role = 'data' AND period_id = $1 AND state = 'active'
		FOR UPDATE`, periodID).Scan(&activeID, &activeVer)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}

	ct, err := tx.Exec(ctx, `
		UPDATE shards SET state = 'sealed', sealed_at = NOW(), updated_at = NOW(), version = version + 1
		WHERE id = $1 AND version = $2`, activeID, activeVer)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrConflict
	}

	var standbyID int64
	err = tx.QueryRow(ctx, `
		SELECT id FROM shards
		WHERE role = 'data' AND state = 'standby' AND period_id IS NULL
		ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&standbyID)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		UPDATE shards SET period_id = $1, state = 'active', updated_at = NOW(), version = version + 1
		WHERE id = $2`, periodID, standbyID)
	if err != nil {
		return err
	}
	_, _ = tx.Exec(ctx, `INSERT INTO shard_events (shard_id, period_id, event_type) VALUES ($1, $2, 'seal_rotate')`, activeID, periodID)
	return tx.Commit(ctx)
}

func (s *Store) UpdateStats(ctx context.Context, shardUUID uuid.UUID, reportedBytes int64) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE shards SET reported_bytes = $1, last_seen_at = NOW(), updated_at = NOW()
		WHERE shard_uuid = $2`, reportedBytes, shardUUID)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) PatchState(ctx context.Context, id int64, newState fsm.State) error {
	sh, err := s.GetShardByID(ctx, id)
	if err != nil {
		return err
	}
	if err := fsm.ValidateTransition(sh.Role, sh.State, newState); err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		UPDATE shards SET state = $1, updated_at = NOW(), version = version + 1 WHERE id = $2`,
		newState, id)
	return err
}

func (s *Store) MarkPeriodCleaning(ctx context.Context, periodID string) (int64, error) {
	ct, err := s.pool.Exec(ctx, `
		UPDATE shards SET state = 'cleaning', updated_at = NOW(), version = version + 1
		WHERE role = 'data' AND period_id = $1 AND state IN ('sealed', 'active')`, periodID)
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
}

func (s *Store) FinishCleaning(ctx context.Context, shardUUID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE shards SET state = 'standby', period_id = NULL, reported_bytes = 0,
			sealed_at = NULL, updated_at = NOW(), version = version + 1
		WHERE shard_uuid = $1 AND state = 'cleaning'`, shardUUID)
	return err
}

func (s *Store) DistinctPeriodIDs(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT period_id FROM shards
		WHERE role = 'data' AND period_id IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var pid *string
		if err := rows.Scan(&pid); err != nil {
			return nil, err
		}
		if pid != nil {
			out = append(out, *pid)
		}
	}
	return out, rows.Err()
}

func (s *Store) ShardsNeedingSeal(ctx context.Context, defaultMax int64) ([]Shard, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+shardCols+` FROM shards
		WHERE role = 'data' AND state = 'active'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Shard
	for rows.Next() {
		sh, err := scanShard(rows)
		if err != nil {
			return nil, err
		}
		max := defaultMax
		if sh.MaxBytes != nil {
			max = *sh.MaxBytes
		}
		if sh.ReportedBytes >= max {
			out = append(out, *sh)
		}
	}
	return out, rows.Err()
}

func (s *Store) CountStandbyPool(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM shards
		WHERE role = 'data' AND state = 'standby' AND period_id IS NULL`).Scan(&n)
	return n, err
}

func (s *Store) AutoPromoteIfNoActive(ctx context.Context, periodID string) (*Shard, error) {
	_, err := s.ActiveForPeriod(ctx, periodID)
	if err == nil {
		return nil, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	return s.PromoteStandbyToActive(ctx, periodID)
}

func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func Endpoint(sh *Shard) string {
	if sh.AdvertiseURL != nil && *sh.AdvertiseURL != "" {
		return *sh.AdvertiseURL
	}
	return sh.DSN
}
