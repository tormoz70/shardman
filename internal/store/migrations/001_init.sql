-- symlink content for embed; copy of ../001_init.sql
CREATE TABLE IF NOT EXISTS cluster_config (
    id              SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    mode            VARCHAR(32) NOT NULL DEFAULT 'range',
    period_axis     VARCHAR(16) NOT NULL,
    period_spec     JSONB NOT NULL,
    shard_max_bytes BIGINT NOT NULL,
    retention_depth INT,
    max_future_periods INT,
    bootstrapped_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS shards (
    id              BIGSERIAL PRIMARY KEY,
    shard_uuid      UUID NOT NULL UNIQUE,
    role            VARCHAR(16) NOT NULL DEFAULT 'data',
    period_id       VARCHAR(64),
    state           VARCHAR(16) NOT NULL DEFAULT 'standby',
    dsn             TEXT NOT NULL,
    advertise_url   TEXT,
    reported_bytes  BIGINT NOT NULL DEFAULT 0,
    max_bytes       BIGINT,
    last_seen_at    TIMESTAMPTZ,
    sealed_at       TIMESTAMPTZ,
    version         BIGINT NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT shards_role_check CHECK (role IN ('data', 'error')),
    CONSTRAINT shards_state_check CHECK (state IN ('standby', 'active', 'sealed', 'cleaning'))
);

CREATE UNIQUE INDEX IF NOT EXISTS shards_one_active_per_period
    ON shards (period_id)
    WHERE role = 'data' AND state = 'active' AND period_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS shards_one_error
    ON shards (role)
    WHERE role = 'error';

CREATE TABLE IF NOT EXISTS shard_events (
    id          BIGSERIAL PRIMARY KEY,
    shard_id    BIGINT REFERENCES shards(id),
    period_id   VARCHAR(64),
    event_type  VARCHAR(32) NOT NULL,
    details     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS shards_period_id_idx ON shards(period_id);
CREATE INDEX IF NOT EXISTS shards_state_idx ON shards(state);
