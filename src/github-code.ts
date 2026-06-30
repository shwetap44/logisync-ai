/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * GITHUB EXPORT CODE SHOWCASE
 *
 * This file defines the "Production Codebase" tab content — a curated list of
 * source files that demonstrate the backend engineering skills in this project.
 * Each entry shows the filename, category, a plain-English description, and
 * the actual source code for recruiters to review.
 */

export interface GitHubFile {
  filename: string;
  category: string;
  description: string;
  language: string;
  code: string;
}

export const GITHUB_EXPORT_FILES: GitHubFile[] = [
  {
    filename: 'server.ts',
    category: 'Express Backend',
    description: 'Main Express HTTP server — defines all 4 REST API routes, integrates Redis rate-limiting middleware, and manages Vite dev server proxy in development mode.',
    language: 'typescript',
    code: `import express from 'express';
import { createServer as createViteServer } from 'vite';
import path from 'path';
import fs from 'fs';
import crypto from 'crypto';
import dotenv from 'dotenv';

dotenv.config();

import { isRateLimited } from './server/redis';
import { workflows, kafkaTopic, consumers, totalThroughput, rebalancePartitions, startPipelineWorker } from './server/kafka';
import { databaseLogs, systemLogs, dbSizeMb, indexHits, sequentialScans, avgQueryLatencyMs, addSystemLog } from './server/postgres';

async function startServer() {
  const app = express();
  app.use(express.json());

  // GET /api/telemetry — returns full system snapshot
  app.get('/api/telemetry', (req, res) => {
    res.json({
      metrics: { totalThroughput, p99Latency: \`\${avgQueryLatencyMs}ms\`, nodesOnline: \`\${consumers.filter(c => c.status !== 'paused').length} / \${consumers.length}\`, dbSizeMb, indexHits: \`\${indexHits}%\`, sequentialScans, avgQueryLatencyMs },
      partitions: kafkaTopic.partitions,
      consumers,
      workflows,
      databaseLogs,
      systemLogs
    });
  });

  // POST /api/workflows — rate-limited job submission
  app.post('/api/workflows', (req, res) => {
    const { title, text } = req.body;
    const clientIp = req.ip || '127.0.0.1';
    if (!title || !text) return res.status(400).json({ error: 'Title and text are required.' });

    const limitCheck = isRateLimited(clientIp, 6, 20000);
    if (limitCheck.limited) {
      return res.status(429).json({ error: 'Too Many Requests', message: 'Redis Sliding-Window Rate Limiter triggered.' });
    }

    const newWorkflow = { id: crypto.randomUUID(), title, text, status: 'pending', currentStage: 'ingestion', timestamp: Date.now() };
    workflows.unshift(newWorkflow as any);
    startPipelineWorker(newWorkflow as any);
    res.status(202).json({ message: 'Workflow queued in Kafka.', workflow: newWorkflow });
  });

  app.listen(3000, '0.0.0.0', () => console.log('LogiSync listening on http://localhost:3000'));
}

startServer();`
  },
  {
    filename: 'server/kafka.ts',
    category: 'Message Queue',
    description: 'Simulates Apache Kafka broker logic — 3 partitions with MD5-based key routing, round-robin consumer rebalancing, and a full async 4-stage pipeline: Ingestion → Summarization → Classification → DB Audit.',
    language: 'typescript',
    code: `import crypto from 'crypto';
import { Workflow, KafkaPartition, Consumer } from './types';
import { addSystemLog, addDatabaseLog } from './postgres';
import { getAiClient } from './gemini';

export const kafkaTopic = {
  name: 'workflow-pipeline',
  partitions: [
    { id: 0, messages: [], offset: 124, committedOffsets: { 'agent-workers': 124 } },
    { id: 1, messages: [], offset: 98,  committedOffsets: { 'agent-workers': 98  } },
    { id: 2, messages: [], offset: 112, committedOffsets: { 'agent-workers': 112 } },
  ] as KafkaPartition[]
};

export let consumers: Consumer[] = [
  { id: 'worker-node-alpha', groupId: 'agent-workers', assignedPartitions: [0], status: 'idle' },
  { id: 'worker-node-beta',  groupId: 'agent-workers', assignedPartitions: [1, 2], status: 'idle' }
];

// Round-robin rebalancing — assigns partitions evenly across active consumers
export function rebalancePartitions() {
  const active = consumers.filter(c => c.status !== 'paused');
  consumers.forEach(c => c.assignedPartitions = []);
  kafkaTopic.partitions.forEach((p, i) => {
    if (active.length > 0) active[i % active.length].assignedPartitions.push(p.id);
  });
}

// 4-stage async pipeline worker
export async function startPipelineWorker(workflow: Workflow) {
  const partitionId = crypto.createHash('md5').update(workflow.title).digest()[0] % 3;
  const partition = kafkaTopic.partitions.find(p => p.id === partitionId)!;
  
  workflow.status = 'queued';
  partition.offset++;
  
  setTimeout(async () => {
    workflow.status = 'processing';
    // ... calls Gemini AI, updates workflow, commits Kafka offsets
    workflow.status = 'completed';
    partition.committedOffsets['agent-workers'] = partition.offset;
  }, 2000);
}`
  },
  {
    filename: 'server/redis.ts',
    category: 'Rate Limiting',
    description: 'Implements a Redis Sliding Window Rate Limiter in pure TypeScript — tracks request timestamps per IP and evicts stale entries outside the rolling window. Returns HTTP 429 when the limit is exceeded.',
    language: 'typescript',
    code: `interface RedisLimitTracker {
  timestamps: number[];
}

const store: Record<string, RedisLimitTracker> = {};

/**
 * Sliding Window Rate Limiter
 * 
 * Algorithm:
 * 1. Load timestamp history for this IP from the in-memory store.
 * 2. Evict all timestamps older than the window (20s).
 * 3. If remaining count >= limit → BLOCK (HTTP 429).
 * 4. Otherwise → ALLOW, record this timestamp.
 *
 * This is equivalent to Redis' ZADD + ZREMRANGEBYSCORE + ZCARD Lua script.
 */
export function isRateLimited(
  ip: string,
  limit: number = 6,
  windowMs: number = 20000
): { limited: boolean; remaining: number } {
  const now = Date.now();
  if (!store[ip]) store[ip] = { timestamps: [] };

  const tracker = store[ip];
  // Evict timestamps outside the sliding window
  tracker.timestamps = tracker.timestamps.filter(ts => now - ts < windowMs);

  if (tracker.timestamps.length >= limit) {
    return { limited: true, remaining: 0 };
  }

  tracker.timestamps.push(now);
  return { limited: false, remaining: limit - tracker.timestamps.length };
}`
  },
  {
    filename: 'server/postgres.ts',
    category: 'Database Layer',
    description: 'Simulates PostgreSQL query logging and index monitoring. Tracks GIN Index Scans vs. Sequential Table Scans, query execution latency, database size growth, and system telemetry events.',
    language: 'typescript',
    code: `import crypto from 'crypto';

export const databaseLogs: DatabaseLog[] = [
  {
    id: crypto.randomUUID(),
    query: "SELECT * FROM workflows WHERE entities @> ARRAY['GIN Index']",
    executionTimeMs: 0.24,
    indexUsed: true,   // GIN index hit — sub-millisecond!
    timestamp: Date.now() - 300000
  }
];

export const systemLogs: SystemLog[] = [];

export let dbSizeMb = 124.5;
export let avgQueryLatencyMs = 12.4;

// Simulates INSERT INTO workflows (...) with GIN index update
export function addDatabaseLog(query: string, ms: number, indexUsed: boolean) {
  databaseLogs.unshift({ id: crypto.randomUUID(), query, executionTimeMs: ms, indexUsed, timestamp: Date.now() });
  if (databaseLogs.length > 50) databaseLogs.pop();
}

// Writes to STDOUT (system_logs table)
export function addSystemLog(level: string, component: string, message: string) {
  systemLogs.unshift({ id: crypto.randomUUID(), timestamp: Date.now(), level, component, message } as any);
  if (systemLogs.length > 50) systemLogs.pop();
}`
  },
  {
    filename: 'server/gemini.ts',
    category: 'AI Integration',
    description: 'Lazy-initializes the Google Gemini 2.5 Flash AI client. Performs two AI tasks per workflow: (1) technical summarization under 80 words, (2) structured JSON classification of sentiment and keyword entities.',
    language: 'typescript',
    code: `import { GoogleGenAI } from '@google/genai';

let aiClient: GoogleGenAI | null = null;

/**
 * Lazy singleton initialization.
 * The client is only created the first time a worker needs it.
 * If no API key is present → returns null → fallback regex parser activates.
 */
export function getAiClient(): GoogleGenAI | null {
  if (!aiClient) {
    const key = process.env.GEMINI_API_KEY;
    if (key && key !== 'PLACEHOLDER_API_KEY' && key.trim() !== '') {
      aiClient = new GoogleGenAI({ apiKey: key });
    }
  }
  return aiClient;
}

// Usage in pipeline worker:
// const ai = getAiClient();
// const summary = await ai.models.generateContent({
//   model: 'gemini-2.5-flash',
//   contents: \`Summarize in under 80 words: \${workflow.text}\`
// });`
  },
  {
    filename: 'server/types.ts',
    category: 'Type Definitions',
    description: 'Shared TypeScript interfaces for the entire backend — Workflow, KafkaPartition, Consumer, KafkaMessage, DatabaseLog, SystemLog, and RedisLimitTracker.',
    language: 'typescript',
    code: `export interface Workflow {
  id: string;
  title: string;
  text: string;
  status: 'pending' | 'queued' | 'processing' | 'completed' | 'failed' | 'dlq';
  currentStage: 'ingestion' | 'summarization' | 'classification' | 'audit';
  summary?: string;
  sentiment?: string;
  entities?: string[];
  error?: string;
  timestamp: number;
}

export interface KafkaPartition {
  id: number;
  messages: KafkaMessage[];
  offset: number;
  committedOffsets: Record<string, number>;
}

export interface Consumer {
  id: string;
  groupId: string;
  assignedPartitions: number[];
  status: 'idle' | 'processing' | 'paused';
  currentMessageId?: string;
}

export interface KafkaMessage {
  id: string;
  topic: string;
  partition: number;
  offset: number;
  payload: Workflow;
  timestamp: number;
  retries: number;
}

export interface DatabaseLog {
  id: string;
  query: string;
  executionTimeMs: number;
  indexUsed: boolean;
  timestamp: number;
}

export interface SystemLog {
  id: string;
  timestamp: number;
  level: 'info' | 'warn' | 'error' | 'success';
  component: 'GATEWAY' | 'KAFKA' | 'REDIS' | 'DATABASE' | 'WORKER';
  message: string;
}

export interface RedisLimitTracker {
  timestamps: number[];
}`
  }
];
