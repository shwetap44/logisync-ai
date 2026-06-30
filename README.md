# LogiSync-AI

LogiSync-AI is a full-stack TypeScript project that demonstrates a real event-driven backend pipeline with a live React dashboard. Users submit technical workflow text through the UI, the backend rate-limits the request with Redis, publishes the job to Kafka, processes it with KafkaJS worker consumers, enriches it with Google Gemini or a local fallback parser, stores the final state in PostgreSQL, and exposes system telemetry back to the dashboard.

The project is backend-heavy. The frontend is a control room for observing queue partitions, worker membership, database trace logs, rate-limit behavior, and processed workflows.

## What This Project Demonstrates

| Area | Implementation |
| --- | --- |
| HTTP gateway | Express server in `server.ts` exposes workflow, telemetry, and consumer-management APIs. |
| Frontend dashboard | React + Vite dashboard in `src/App.tsx` polls telemetry every 3 seconds. |
| Message queue | KafkaJS producer and consumers use the `workflow-pipeline` topic and `agent-workers` consumer group. |
| Worker orchestration | Initial `worker-node-alpha` and `worker-node-beta` consumers are started at boot; more workers can be added from the API/UI. |
| Rate limiting | Redis sorted sets track request timestamps per IP for a rolling 6 requests / 20 seconds limit. |
| AI enrichment | Gemini 2.5 Flash summarizes and classifies workflow text when `GEMINI_API_KEY` is available. |
| Fallback processing | If Gemini is not configured or fails, a local parser creates a short summary, sentiment, and extracted entities. |
| Persistence | PostgreSQL stores workflow state, system logs, and database query audit logs. |
| Indexing | PostgreSQL uses a GIN index for workflow `entities TEXT[]` and B-tree indexes for status/time-based dashboard reads. |

## Repository Layout

```text
LogiSync-AI/
├── server.ts                  # Express app, API routes, Vite middleware, startup sequence
├── server/
│   ├── kafka.ts               # Kafka producer, admin client, consumers, worker pipeline
│   ├── redis.ts               # Redis client and sliding-window rate limiter
│   ├── postgres.ts            # PostgreSQL read/write service functions and query logging
│   ├── gemini.ts              # Lazy Gemini SDK initialization
│   ├── types.ts               # Shared TypeScript interfaces
│   └── db/
│       ├── client.ts          # PostgreSQL connection pool
│       ├── init.ts            # Database creation and schema bootstrap script
│       └── schema.sql         # Tables, indexes, and seed data
├── src/
│   ├── App.tsx                # React dashboard and user interactions
│   ├── main.tsx               # React app mount
│   ├── index.css              # Tailwind entry and custom scrollbar utilities
│   └── github-code.ts         # Curated code showcase data, currently not mounted in the UI
├── docker-compose.yml         # Local Redis and Kafka infrastructure
├── package.json               # Scripts and dependencies
├── vite.config.ts             # Vite + React configuration
├── tailwind.config.js         # Tailwind content paths
└── tsconfig.json              # TypeScript configuration
```

## High-Level Architecture

```mermaid
flowchart LR
  User["User / Browser"] --> UI["React Dashboard<br/>src/App.tsx"]
  UI -->|GET /api/telemetry every 3s| API["Express Gateway<br/>server.ts"]
  UI -->|POST /api/workflows| API
  UI -->|POST /api/consumers/*| API

  API -->|Sliding-window check| Redis["Redis 7<br/>Sorted-set rate limit keys"]
  API -->|Produce workflow message| Kafka["Kafka 3.7<br/>topic: workflow-pipeline<br/>3 partitions"]

  Kafka --> Consumers["KafkaJS Consumers<br/>group: agent-workers"]
  Consumers -->|Summarize + classify| Gemini["Google Gemini 2.5 Flash<br/>optional"]
  Consumers -->|Fallback when no API key / AI error| Fallback["Local parser"]
  Consumers --> Postgres["PostgreSQL<br/>workflows, logs, query audits"]

  API -->|Read snapshots| Postgres
  API -->|Fetch offsets / lag| Kafka
  API -->|Return JSON telemetry| UI
```

## Runtime Sequence

```mermaid
sequenceDiagram
  autonumber
  participant B as Browser Dashboard
  participant E as Express API
  participant R as Redis
  participant K as Kafka
  participant W as KafkaJS Worker
  participant A as Gemini / Fallback
  participant P as PostgreSQL

  B->>E: POST /api/workflows { title, text }
  E->>R: ZREMRANGEBYSCORE + ZCARD + EXPIRE
  alt rate limit exceeded
    E->>P: INSERT system_logs rate-limit event
    E-->>B: 429 Too Many Requests
  else request allowed
    E->>R: ZADD request timestamp
    E->>K: produce message to workflow-pipeline
    E->>P: INSERT system_logs gateway event
    E-->>B: 202 Accepted
  end

  K-->>W: Deliver message to assigned partition consumer
  W->>P: UPSERT workflow status=processing stage=summarization
  W->>A: Generate summary and classification
  A-->>W: summary, sentiment, entities
  W->>P: UPSERT workflow status=completed stage=audit
  W->>P: INSERT database_query_logs and system_logs
  W->>K: Commit offset after successful processing

  loop every 3 seconds
    B->>E: GET /api/telemetry
    E->>P: Read workflows, system logs, DB query logs, metrics
    E->>K: Fetch partition and committed offsets
    E-->>B: Render dashboard snapshot
  end
```

## Startup Flow

```mermaid
flowchart TD
  A["npm start / npm run dev"] --> B["server.ts main()"]
  B --> C["Load .env"]
  C --> D["Connect Redis eagerly<br/>non-fatal if retrying"]
  D --> E["initKafka()"]
  E --> F["Connect Kafka admin + producer"]
  F --> G{"Does topic exist?"}
  G -- No --> H["Create workflow-pipeline<br/>3 partitions, replicationFactor=1"]
  G -- Yes --> I["Reuse existing topic"]
  H --> J["Spawn worker-node-alpha"]
  I --> J
  J --> K["Spawn worker-node-beta"]
  K --> L["Write startup system log"]
  L --> M["Start Express on PORT or 3000"]
  M --> N{"NODE_ENV"}
  N -- development --> O["Attach Vite middleware"]
  N -- production --> P["Serve dist/ static files"]
```

## Backend Components

### `server.ts` - Express Gateway

`server.ts` is the application entry point. It creates the Express app, wires JSON parsing, exposes all API routes, starts Vite middleware in development, serves the built `dist/` directory in production, and runs the startup sequence.

Routes:

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/api/telemetry` | Returns workflows, Kafka partition state, consumers, database logs, system logs, and computed metrics. |
| `POST` | `/api/workflows` | Validates `title` and `text`, rate-limits the client IP in Redis, creates a workflow ID, and produces the workflow to Kafka. |
| `POST` | `/api/consumers/toggle` | Disconnects an active Kafka consumer or restarts a paused one, causing Kafka group rebalancing. |
| `POST` | `/api/consumers/add` | Adds a new consumer node from the available names `gamma`, `delta`, `epsilon`, and `zeta`. |

### `server/redis.ts` - Sliding-Window Rate Limiter

Redis stores one sorted set per client IP:

```text
key:    logisync:ratelimit:{ip}
member: {timestamp}-{uuid}
score:  request timestamp in milliseconds
```

For every workflow submission:

1. Remove old timestamps with `ZREMRANGEBYSCORE key 0 windowStart`.
2. Count active timestamps with `ZCARD key`.
3. Set a short TTL with `EXPIRE key`.
4. If active count is already at the limit, block with HTTP `429`.
5. Otherwise, record the new timestamp with `ZADD`.

Current limit: `6` accepted workflow submissions per `20,000 ms` per IP.

Implementation note: the cleanup/count/expire commands are sent through an ioredis pipeline for fewer network round trips. The code does not currently use a Redis Lua script or Redis transaction, so extremely concurrent requests from the same IP may observe a small race window.

### `server/kafka.ts` - Queue and Workers

Kafka responsibilities are centralized in `server/kafka.ts`.

| Object | Role |
| --- | --- |
| `Kafka` client | Connects to `KAFKA_BROKERS`, defaulting to `localhost:9092`. |
| `admin` | Creates the topic and fetches topic/group offsets for the dashboard. |
| `producer` | Sends workflow messages to the `workflow-pipeline` topic. |
| `consumerRegistry` | Keeps the in-memory list of known worker nodes and their UI state. |
| `spawnConsumer()` | Creates a KafkaJS consumer in group `agent-workers`. |
| `stopConsumer()` | Disconnects a consumer and marks it paused. |
| `restartConsumer()` | Removes the paused consumer object and creates a fresh consumer with the same ID. |
| `getKafkaPartitionState()` | Reads log-end offsets and group committed offsets, cached for 8 seconds. |
| `runPipeline()` | Processes one workflow message through the AI and persistence stages. |

Kafka topic details:

```text
topic:              workflow-pipeline
partitions:         3
replicationFactor:  1
consumer group:     agent-workers
initial consumers:  worker-node-alpha, worker-node-beta
commit strategy:    manual commit after pipeline completion
```

Workflow messages use the workflow title as the Kafka message key. KafkaJS's default partitioner chooses the partition for that key, which gives same-key ordering behavior within a partition.

### Worker Pipeline

```mermaid
stateDiagram-v2
  [*] --> queued: Express creates workflow
  queued --> processing: Kafka consumer receives message
  processing --> summarization: upsert status=processing
  summarization --> classification: Gemini summary complete or fallback starts
  classification --> audit: sentiment/entities available
  audit --> completed: final PostgreSQL upsert + query audit log
  summarization --> dlq: unhandled pipeline error
  classification --> dlq: unhandled pipeline error
  audit --> dlq: database/write error
```

Worker processing stages:

1. The consumer receives a Kafka message and marks its node status as `processing`.
2. The workflow is upserted to PostgreSQL with `status='processing'` and `current_stage='summarization'`.
3. If Gemini is available, the worker asks `gemini-2.5-flash` for:
   - an under-80-word technical summary
   - JSON classification containing `sentiment` and `entities`
4. If Gemini is unavailable or errors, the worker uses local fallback logic:
   - summary from the first two sentences
   - sentiment based on a simple failure keyword check
   - entity extraction from known backend/system-design terms
5. The worker updates the workflow to `status='completed'` and `current_stage='audit'`.
6. The worker inserts database query audit and system log records.
7. The consumer manually commits the Kafka offset.
8. If an unhandled error occurs, the workflow is marked `dlq`, the error is persisted, and the offset is still committed to avoid repeated processing of the same failing message.

### `server/postgres.ts` - Persistence Service

The PostgreSQL service exposes dashboard reads and pipeline writes:

| Function | What it does |
| --- | --- |
| `getWorkflows()` | Returns latest 20 workflows ordered by `created_at DESC`; logs the dashboard query. |
| `getSystemLogs()` | Returns latest 50 system log entries. |
| `getDatabaseLogs()` | Returns latest 20 query audit entries. |
| `getMetrics()` | Returns database size, index-hit percentage from query logs, sequential scan count, and average query latency. |
| `upsertWorkflow()` | Inserts or updates a workflow by UUID. |
| `addSystemLog()` | Inserts service events from Gateway, Kafka, Redis, Database, and Worker components. |
| `logDbQuery()` | Inserts query text, latency, and whether the query used an index. |

The database pool is configured in `server/db/client.ts` with:

```text
max connections:        10
idle timeout:           30 seconds
connection timeout:     5 seconds
connection string env:  DATABASE_URL
```

### `server/gemini.ts` - AI Client

The Gemini SDK is lazy-initialized. The client is only created when a worker first needs AI processing. This keeps startup resilient when `GEMINI_API_KEY` is missing, empty, or left as the placeholder `MY_GEMINI_API_KEY`.

When no valid key is present, `getAiClient()` returns `null` and the worker automatically uses the local fallback parser.

## Storage Design

LogiSync-AI stores durable application state in PostgreSQL, short-lived rate-limit counters in Redis, and transient queued work in Kafka.

```mermaid
flowchart TB
  subgraph Redis["Redis"]
    R1["logisync:ratelimit:{ip}<br/>sorted set<br/>score = timestamp<br/>member = timestamp-uuid"]
  end

  subgraph Kafka["Kafka"]
    K1["workflow-pipeline<br/>partition 0"]
    K2["workflow-pipeline<br/>partition 1"]
    K3["workflow-pipeline<br/>partition 2"]
    K4["consumer offsets<br/>group: agent-workers"]
  end

  subgraph Postgres["PostgreSQL"]
    P1["workflows<br/>workflow state + AI output"]
    P2["system_logs<br/>service events"]
    P3["database_query_logs<br/>query audit telemetry"]
  end

  R1 -. protects .-> P1
  K1 --> P1
  K2 --> P1
  K3 --> P1
  K4 -. observed by .-> P3
```

### PostgreSQL Schema Diagram

```mermaid
erDiagram
  WORKFLOWS {
    uuid id PK
    text title
    text text
    varchar status
    varchar current_stage
    text summary
    varchar sentiment
    text_array entities
    text error
    timestamptz created_at
  }

  SYSTEM_LOGS {
    uuid id PK
    varchar level
    varchar component
    text message
    timestamptz created_at
  }

  DATABASE_QUERY_LOGS {
    uuid id PK
    text query
    decimal execution_time_ms
    boolean index_used
    timestamptz created_at
  }
```

There are no foreign keys between these tables. They are intentionally independent telemetry tables:

- `workflows` is the source of truth for submitted jobs and AI results.
- `system_logs` is an append-only event stream for service-level messages.
- `database_query_logs` is an append-only trace table for dashboard query/index telemetry.

Index:

| Index | Type | Purpose |
| --- | --- | --- |
| `idx_db_query_logs_created_at` | `B-tree(created_at DESC)` | Fast latest-query dashboard reads. |

## Data Lifecycle

```mermaid
flowchart LR
  A["Submitted text"] --> B["Workflow object<br/>id, title, text, status=queued"]
  B --> C["Kafka message<br/>key=title<br/>value=workflow JSON"]
  C --> D["Worker processing<br/>status=processing"]
  D --> E["AI enrichment<br/>summary, sentiment, entities"]
  E --> F["PostgreSQL workflows row<br/>status=completed"]
  F --> G["Dashboard table<br/>summary + tags + age"]

  D --> H["system_logs"]
  E --> H
  F --> I["database_query_logs"]
  H --> J["Telemetry response"]
  I --> J
  F --> J
```

## Frontend Behavior

The frontend is implemented in `src/App.tsx` as a single dashboard view.

Main UI sections:

| Section | Data source | Behavior |
| --- | --- | --- |
| Header metrics | `GET /api/telemetry` | Displays throughput, latency, live workers, and index hit rate. |
| Job submission panel | `POST /api/workflows` | Lets users submit custom workflow text or apply sample system-design presets. |
| Kafka topology panel | `GET /api/telemetry` | Displays partition log-end offsets, committed offsets, lag, and consumer assignments. |
| Consumer controls | `POST /api/consumers/toggle`, `POST /api/consumers/add` | Lets users pause/restart workers and add additional consumer nodes. |
| Workflow table | `GET /api/telemetry` | Shows latest workflow status, pipeline stage, summary, sentiment, and entities. |
| PostgreSQL trace logger | `GET /api/telemetry` | Shows query audit records and whether each was marked as index-backed. |
| System telemetry log | `GET /api/telemetry` | Shows latest service logs from backend components. |

The dashboard polls `/api/telemetry` every 3 seconds. After successful submissions or consumer changes, it also refreshes immediately.

## API Reference

### `GET /api/telemetry`

Returns a full dashboard snapshot.

```bash
curl http://localhost:3000/api/telemetry
```

Response shape:

```json
{
  "metrics": {
    "totalThroughput": 142.8,
    "p99Latency": "12.4ms",
    "nodesOnline": "2 / 2",
    "dbSizeMb": 1,
    "indexHits": "98.2%",
    "sequentialScans": 0,
    "avgQueryLatencyMs": 12.4
  },
  "partitions": [],
  "consumers": [],
  "workflows": [],
  "databaseLogs": [],
  "systemLogs": []
}
```

### `POST /api/workflows`

Submits a workflow to the Kafka pipeline.

```bash
curl -X POST http://localhost:3000/api/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "title": "PostgreSQL GIN Indexes for Tag Search",
    "text": "A Generalized Inverted Index maps individual array tokens to matching row IDs for fast tag search."
  }'
```

Success response: `202 Accepted`

```json
{
  "message": "Workflow accepted and produced to Kafka topic \"workflow-pipeline\".",
  "workflow": {
    "id": "generated-uuid",
    "title": "PostgreSQL GIN Indexes for Tag Search",
    "text": "A Generalized Inverted Index maps individual array tokens to matching row IDs for fast tag search.",
    "status": "queued",
    "currentStage": "ingestion",
    "timestamp": 1710000000000
  }
}
```

Rate-limit response: `429 Too Many Requests`

```json
{
  "error": "Too Many Requests",
  "message": "Redis Sliding-Window Rate Limiter triggered. Maximum of 6 requests per 20 seconds is permitted."
}
```

### `POST /api/consumers/toggle`

Disconnects an active consumer or restarts a paused consumer.

```bash
curl -X POST http://localhost:3000/api/consumers/toggle \
  -H "Content-Type: application/json" \
  -d '{"consumerId": "worker-node-alpha"}'
```

### `POST /api/consumers/add`

Adds a new worker node. The code picks the first unused suffix from `gamma`, `delta`, `epsilon`, and `zeta`.

```bash
curl -X POST http://localhost:3000/api/consumers/add
```

If all available names are used, the API returns `400` with:

```json
{
  "error": "Cluster capacity limit reached. Maximum 4 consumer workers."
}
```

Note: the current implementation starts with 2 workers and can add 4 more named workers, so the effective maximum is 6 total registered workers even though the error message says 4.

## Local Development

### Prerequisites

- Node.js 18+ or 20+
- npm
- Docker Desktop or compatible Docker runtime
- PostgreSQL reachable from `DATABASE_URL`
- Optional Gemini API key from Google AI Studio

### Environment Variables

Create `.env` in the project root:

```env
PORT=3000
NODE_ENV=development

DATABASE_URL=postgresql://user:pass@localhost:5433/logisync
REDIS_URL=redis://localhost:6379
KAFKA_BROKERS=localhost:9092

GEMINI_API_KEY=your_gemini_api_key_here
```

Notes:

- `GEMINI_API_KEY` is optional. Without it, the worker uses local fallback processing.
- `server/db/client.ts` and `server/db/init.ts` also load `.env.local` with override behavior. If both `.env` and `.env.local` exist, database-related settings may come from `.env.local`.
- The included `docker-compose.yml` starts Redis and Kafka only. PostgreSQL must be supplied separately unless you extend the Compose file.

### Install

```bash
npm install
```

### Start Redis and Kafka

```bash
npm run infra:up
```

This starts:

| Service | Container | Port |
| --- | --- | --- |
| Redis 7 Alpine | `logisync-redis` | `6379` |
| Apache Kafka 3.7 KRaft | `logisync-kafka` | `9092` |

### Initialize PostgreSQL

```bash
npm run db:init
```

The script:

1. Reads `DATABASE_URL`.
2. Connects to the default `postgres` database.
3. Creates the target database if it does not exist.
4. Runs `server/db/schema.sql`.
5. Creates indexes and seed rows.

### Run the App

```bash
npm start
```

Open:

```text
http://localhost:3000
```

In development, Express attaches Vite as middleware, so the backend API and frontend are served from the same port.

### Production Build

```bash
npm run build
NODE_ENV=production npm start
```

In production mode, Express serves the compiled `dist/` assets.

## Operational Details

### Kafka Offset and Lag Telemetry

`getKafkaPartitionState()` fetches:

- topic log-end offsets via `admin.fetchTopicOffsets(TOPIC)`
- committed group offsets via `admin.fetchOffsets({ groupId, topics: [TOPIC] })`

The result is cached for 8 seconds to avoid hitting Kafka Admin on every dashboard poll. The cache is invalidated when consumers join, rebalance, or process messages.

Dashboard lag is calculated as:

```text
partition.offset - committedOffsets["agent-workers"]
```

### Consumer Rebalancing

The dashboard can pause and restart workers. Pausing calls `consumer.disconnect()`, which causes Kafka to rebalance partitions among remaining active members. Restarting removes the stale registry entry and spawns a new KafkaJS consumer with the same display ID.

KafkaJS group events update `assignedPartitions` in memory:

- `GROUP_JOIN` records the new partition assignment.
- `REBALANCING` logs a rebalance event and invalidates cached partition telemetry.

### Delivery Semantics

The consumer runs with `autoCommit: false`. It commits offsets manually after pipeline completion. If the worker throws an error, the code marks the workflow as `dlq`, persists the error, and still commits the offset. This gives the project at-least-once style processing for successful jobs while avoiding infinite retries for failed jobs.

### Metrics

Some metrics are measured from real systems and some are demonstration telemetry:

| Metric | Source |
| --- | --- |
| `dbSizeMb` | Real `pg_database_size(current_database())`. |
| `indexHits` | Computed from `database_query_logs.index_used`. |
| `sequentialScans` | Count of query logs where `index_used=false`. |
| `avgQueryLatencyMs` | Average of `database_query_logs.execution_time_ms`. |
| `nodesOnline` | Consumer registry statuses. |
| `p99Latency` | Currently populated from average query latency, not a true percentile calculation. |
| `totalThroughput` | Demonstration counter initialized around `142.8` and randomized after completed jobs. |

## Known Implementation Notes

- The Redis limiter is implemented with sorted sets and pipelines, not Lua.
- The UI has a few labels that describe aspirational internals such as Lua or MD5. The backend code currently uses ioredis pipelines and KafkaJS's default partitioner.
- `src/github-code.ts` contains older curated code snippets and is not currently rendered by `App.tsx`.
- `POST /api/consumers/add` can add four extra workers on top of the initial two, while its capacity error message says "Maximum 4 consumer workers."
- PostgreSQL must be available separately; the Compose file only provides Redis and Kafka.
- `gen_random_uuid()` is used in the schema. On older PostgreSQL installations, ensure UUID generation support is available.

## Useful Commands

```bash
npm run infra:up       # Start Redis and Kafka
npm run infra:logs     # Follow infrastructure logs
npm run infra:down     # Stop Redis and Kafka
npm run db:init        # Create database, tables, indexes, and seed data
npm start              # Start Express + Vite middleware
npm run build          # Build frontend assets
npm run preview        # Preview Vite build separately
```

## Troubleshooting

### `DATABASE_URL environment variable is not set`

Create `.env` or `.env.local` with a valid `DATABASE_URL`.

### Kafka startup fails

Make sure Docker is running and the Kafka container is healthy:

```bash
docker-compose ps
docker-compose logs -f kafka
```

### Redis connection errors

Check that Redis is running on the configured port:

```bash
docker-compose ps redis
```

### Dashboard is empty

Run:

```bash
npm run db:init
```

Then restart the server. The schema script inserts seed workflows, system logs, and database query logs.

### Gemini is not producing summaries

Confirm `GEMINI_API_KEY` is set and is not the placeholder value. If no key is present, fallback processing is expected and the app should still complete workflows.

## License

Apache-2.0
