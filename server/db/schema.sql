-- LogiSync-AI Database Schema
-- Run once to set up all tables and indexes.

-- ── WORKFLOWS ───────────────────────────────────────────────────────────────
-- Stores every submitted job and its pipeline processing state.
CREATE TABLE IF NOT EXISTS workflows (
  id            UUID          PRIMARY KEY,
  title         TEXT          NOT NULL,
  text          TEXT          NOT NULL,
  status        VARCHAR(20)   NOT NULL DEFAULT 'pending',
  current_stage VARCHAR(30)   NOT NULL DEFAULT 'ingestion',
  summary       TEXT,
  sentiment     VARCHAR(10),
  entities      TEXT[],          -- Array of keyword tags (GIN indexed below)
  error         TEXT,
  created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- GIN index: makes "entities @> ARRAY['tag']" queries sub-millisecond
-- This is the core PostgreSQL feature we're demonstrating!
CREATE INDEX IF NOT EXISTS idx_workflows_entities_gin
  ON workflows USING GIN(entities);

-- B-Tree index: fast lookup by status (used in dashboard queries)
CREATE INDEX IF NOT EXISTS idx_workflows_status
  ON workflows(status);

-- B-Tree index: fast ORDER BY created_at DESC
CREATE INDEX IF NOT EXISTS idx_workflows_created_at
  ON workflows(created_at DESC);


-- ── SYSTEM LOGS ─────────────────────────────────────────────────────────────
-- Records events from all microservices (Gateway, Kafka, Redis, Database, Worker).
CREATE TABLE IF NOT EXISTS system_logs (
  id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  level       VARCHAR(10) NOT NULL,   -- info | warn | error | success
  component   VARCHAR(20) NOT NULL,   -- GATEWAY | KAFKA | REDIS | DATABASE | WORKER
  message     TEXT        NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_system_logs_created_at
  ON system_logs(created_at DESC);


-- ── DATABASE QUERY LOGS ─────────────────────────────────────────────────────
-- Tracks every SQL query run in the pipeline, showing index usage & latency.
-- This is what the "PostgreSQL Real-Time Trace Logger" panel displays.
CREATE TABLE IF NOT EXISTS database_query_logs (
  id                UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
  query             TEXT          NOT NULL,
  execution_time_ms DECIMAL(10,3) NOT NULL,
  index_used        BOOLEAN       NOT NULL DEFAULT false,
  created_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_db_query_logs_created_at
  ON database_query_logs(created_at DESC);


-- ── SEED: INITIAL DATA ───────────────────────────────────────────────────────
-- Pre-populate with 2 sample completed workflows so the dashboard isn't empty.
INSERT INTO workflows (id, title, text, status, current_stage, summary, sentiment, entities, created_at)
VALUES
  (
    'f47ac10b-58cc-4372-a567-0e02b2c3d479',
    'PostgreSQL GIN Indexes for Array Searching',
    'A Generalized Inverted Index (GIN) is designed for handling data types that contain multiple values within a single row, such as arrays, jsonb, and full-text documents.',
    'completed',
    'audit',
    'GIN indexes map individual sub-elements of multi-value fields to row IDs, enabling sub-linear query times for array and JSONB searches.',
    'POSITIVE',
    ARRAY['PostgreSQL', 'GIN Index', 'B-Tree', 'jsonb', 'Array Searching'],
    NOW() - INTERVAL '10 minutes'
  ),
  (
    '8a11bc32-11ef-4011-891c-e902b200d918',
    'Distributed System Consensus using Raft',
    'Raft is a consensus algorithm designed as an alternative to Paxos. It manages a replicated log through three state engines: Leader election, Log replication, and Safety constraint loops.',
    'completed',
    'audit',
    'Raft manages replicated logs through dynamic state machines, including leader election, log replication, and core safety constraints.',
    'NEUTRAL',
    ARRAY['Consensus', 'Raft', 'Paxos', 'Replicated Log', 'State Machines'],
    NOW() - INTERVAL '5 minutes'
  )
ON CONFLICT (id) DO NOTHING;

INSERT INTO system_logs (level, component, message)
VALUES
  ('info',    'GATEWAY',  'LogiSync Gateway initialised on Port 3000.'),
  ('success', 'KAFKA',    'Kafka Broker connected. Topic "workflow-pipeline" created with 3 partitions.'),
  ('info',    'REDIS',    'Connected to Redis. Rate-limiter sorted-set keys ready.'),
  ('info',    'DATABASE', 'PostgreSQL connection pool established. GIN and composite indexes verified.')
ON CONFLICT DO NOTHING;

INSERT INTO database_query_logs (query, execution_time_ms, index_used)
VALUES
  ('SELECT * FROM workflows WHERE status = ''completed'' ORDER BY created_at DESC LIMIT 10', 0.24, true),
  ('SELECT * FROM workflows WHERE entities @> ARRAY[''GIN Index'']', 0.11, true)
ON CONFLICT DO NOTHING;
