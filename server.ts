/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 *
 * LogiSync-AI — Express Server
 * Connects to real PostgreSQL, Redis, and Kafka on startup.
 */

import express from 'express';
import { createServer as createViteServer } from 'vite';
import path from 'path';
import fs from 'fs';
import crypto from 'crypto';
import dotenv from 'dotenv';

// Load environment variables
dotenv.config();
dotenv.config({ path: '.env.local' });

// Real service imports
import { isRateLimited, redis } from './server/redis';
import {
  getWorkflows,
  getSystemLogs,
  getDatabaseLogs,
  getMetrics,
  addSystemLog,
} from './server/postgres';
import {
  initKafka,
  produceWorkflow,
  getKafkaPartitionState,
  consumerRegistry,
  spawnConsumer,
  stopConsumer,
  restartConsumer,
  getThroughput,
} from './server/kafka';

// ── APP LAUNCH ENGINE ─────────────────────────────────────────────────────────

async function startServer() {
  const app = express();
  app.use(express.json());

  // ── API ROUTES ──────────────────────────────────────────────────────────────

  // 1. Telemetry dashboard — reads live data from real Postgres + Kafka Admin
  app.get('/api/telemetry', async (req, res) => {
    try {
      const [workflows, systemLogs, databaseLogs, dbMetrics, partitions] =
        await Promise.all([
          getWorkflows(),
          getSystemLogs(),
          getDatabaseLogs(),
          getMetrics(),
          getKafkaPartitionState(),
        ]);

      const consumers = consumerRegistry.map(c => ({
        id: c.id,
        groupId: c.groupId,
        assignedPartitions: c.assignedPartitions,
        status: c.status,
      }));

      res.json({
        metrics: {
          totalThroughput: getThroughput(),
          p99Latency: `${dbMetrics.avgQueryLatencyMs}ms`,
          nodesOnline: `${consumers.filter(c => c.status !== 'paused').length} / ${consumers.length}`,
          ...dbMetrics,
        },
        partitions,
        consumers,
        workflows,
        databaseLogs,
        systemLogs,
      });
    } catch (err: any) {
      console.error('[Telemetry] Error:', err.message);
      res.status(500).json({ error: 'Failed to fetch telemetry data.' });
    }
  });

  // 2. Submit workflow — rate-limited by real Redis, queued to real Kafka
  app.post('/api/workflows', async (req, res) => {
    const { title, text } = req.body;
    const clientIp = req.ip || '127.0.0.1';

    if (!title || !text) {
      return res.status(400).json({ error: 'Title and text fields are required.' });
    }

    // Real Redis sliding window check
    const limitCheck = await isRateLimited(clientIp, 6, 20000);

    if (limitCheck.limited) {
      await addSystemLog('error', 'REDIS',
        `Rate Limit Exceeded for IP ${clientIp}. Blocking request "${title}".`
      );
      res.setHeader('X-RateLimit-Limit', '6');
      res.setHeader('X-RateLimit-Remaining', '0');
      res.setHeader('Retry-After', '20');
      return res.status(429).json({
        error: 'Too Many Requests',
        message: 'Redis Sliding-Window Rate Limiter triggered. Maximum of 6 requests per 20 seconds is permitted.',
      });
    }

    res.setHeader('X-RateLimit-Limit', '6');
    res.setHeader('X-RateLimit-Remaining', String(limitCheck.remaining));

    const newWorkflow = {
      id: crypto.randomUUID(),
      title,
      text,
      status: 'queued' as const,
      currentStage: 'ingestion' as const,
      timestamp: Date.now(),
    };

    // Produce to real Kafka topic — consumer picks it up asynchronously
    await produceWorkflow(newWorkflow);

    res.status(202).json({
      message: 'Workflow accepted and produced to Kafka topic "workflow-pipeline".',
      workflow: newWorkflow,
    });
  });

  // 3. Toggle consumer node — disconnects/reconnects real Kafka consumer (triggers rebalance)
  app.post('/api/consumers/toggle', async (req, res) => {
    const { consumerId } = req.body;
    const consumer = consumerRegistry.find(c => c.id === consumerId);

    if (!consumer) {
      return res.status(404).json({ error: 'Consumer node not found.' });
    }

    if (consumer.status === 'paused') {
      await restartConsumer(consumerId); // Creates a fresh consumer, rejoins group
    } else {
      await stopConsumer(consumerId);   // Disconnects — Kafka rebalances remaining consumers
    }

    res.json({
      consumers: consumerRegistry.map(c => ({
        id: c.id, groupId: c.groupId,
        assignedPartitions: c.assignedPartitions, status: c.status,
      })),
    });
  });

  // 4. Add a consumer node dynamically — spawns a real new KafkaJS consumer
  app.post('/api/consumers/add', async (req, res) => {
    const nodeNames = ['gamma', 'delta', 'epsilon', 'zeta'];
    const unusedName = nodeNames.find(
      name => !consumerRegistry.some(c => c.id === `worker-node-${name}`)
    );

    if (!unusedName) {
      return res.status(400).json({ error: 'Cluster capacity limit reached. Maximum 4 consumer workers.' });
    }

    const newId = `worker-node-${unusedName}`;
    await spawnConsumer(newId);

    res.json({
      consumers: consumerRegistry.map(c => ({
        id: c.id, groupId: c.groupId,
        assignedPartitions: c.assignedPartitions, status: c.status,
      })),
    });
  });

  // ── FRONTEND SERVING ────────────────────────────────────────────────────────

  const isProd = process.env.NODE_ENV === 'production';
  const port = parseInt(process.env.PORT ?? '3000');

  if (!isProd) {
    const vite = await createViteServer({
      configFile: path.resolve('.', 'vite.config.ts'),
      server: { middlewareMode: true },
      appType: 'custom',
    });

    app.use(vite.middlewares);

    app.use('*', async (req, res, next) => {
      const url = req.originalUrl;
      try {
        let template = fs.readFileSync(path.resolve('.', 'index.html'), 'utf-8');
        template = await vite.transformIndexHtml(url, template);
        res.status(200).set({ 'Content-Type': 'text/html' }).end(template);
      } catch (e) {
        vite.ssrFixStacktrace(e as Error);
        next(e);
      }
    });
  } else {
    const distPath = path.resolve('.', 'dist');
    app.use(express.static(distPath));
    app.use('*', (req, res) => {
      res.sendFile(path.resolve(distPath, 'index.html'));
    });
  }

  app.listen(port, '0.0.0.0', () => {
    console.log(`\n🚀 LogiSync Server running at http://localhost:${port}\n`);
  });
}

// ── STARTUP SEQUENCE ──────────────────────────────────────────────────────────

async function main() {
  console.log('[LogiSync] Starting up — connecting to PostgreSQL, Redis, and Kafka...');
  try {
    // Connect Redis eagerly so errors show immediately
    await redis.connect();
  } catch {
    // ioredis will auto-retry; non-fatal at startup
  }

  // Initialise Kafka (creates topic + starts consumers)
  await initKafka();

  await addSystemLog('info', 'GATEWAY', `LogiSync Gateway initialised. All services connected.`);

  // Start Express server
  await startServer();
}

main().catch(err => {
  console.error('[LogiSync] Fatal startup error:', err);
  process.exit(1);
});
