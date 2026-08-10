ALTER TABLE cluster_config ADD COLUMN IF NOT EXISTS topology_version BIGINT NOT NULL DEFAULT 0;

ALTER TABLE shards DROP CONSTRAINT IF EXISTS shards_state_check;
ALTER TABLE shards ADD CONSTRAINT shards_state_check
    CHECK (state IN ('standby', 'active', 'draining', 'sealed', 'cleaning'));

ALTER TABLE shards ADD COLUMN IF NOT EXISTS drain_started_at TIMESTAMPTZ;
ALTER TABLE shards ADD COLUMN IF NOT EXISTS drain_ready BOOLEAN NOT NULL DEFAULT FALSE;
