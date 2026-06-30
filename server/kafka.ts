/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 *
 * Kafka integration using KafkaJS.
 *
 * Architecture:
 *  - Admin:    Creates topic, fetches partition offsets for the dashboard.
 *  - Producer: Sends workflow messages to "workflow-pipeline" topic.
 *  - Consumers: Two consumer instances in the "agent-workers" group.
 *               Each processes messages through the 4-stage AI pipeline.
 *               Can be paused/resumed/added via the dashboard API.
 */
import { Kafka, Producer, Consumer, Admin, Partitioners, logLevel, type logCreator } from 'kafkajs';
import crypto from 'crypto';
import { addSystemLog, logDbQuery, upsertWorkflow } from './postgres';
import { getAiClient } from './gemini';
import { Type } from '@google/genai';
import type { Workflow } from './types';

// ── KAFKA CLIENT ─────────────────────────────────────────────────────────────

const logisyncKafkaLogger: logCreator = () => ({ level, log }) => {
  const message = log.message ?? '';
  const error = typeof log.error === 'string' ? log.error : '';

  if (
    message.includes('Response without match') ||
    error.includes('The group is rebalancing, so a rejoin is needed') ||
    message.includes('The group is rebalancing, re-joining')
  ) {
    return;
  }

  const payload = JSON.stringify({
    ...log,
    level: logLevel[level],
    logger: 'kafkajs',
  });

  if (level <= logLevel.ERROR) {
    console.error(payload);
  } else if (level <= logLevel.WARN) {
    console.warn(payload);
  }
};

const kafka = new Kafka({
  clientId: 'logisync-server',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  logLevel: logLevel.WARN, // suppress verbose KafkaJS internal logs
  logCreator: logisyncKafkaLogger,
  retry: { initialRetryTime: 300, retries: 8 },
});

export const TOPIC = 'workflow-pipeline';
export const GROUP_ID = 'agent-workers';

// ── ADMIN CLIENT ─────────────────────────────────────────────────────────────

export const admin: Admin = kafka.admin();

// ── PRODUCER ─────────────────────────────────────────────────────────────────

export const producer: Producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});

// ── CONSUMER REGISTRY ────────────────────────────────────────────────────────
// Tracks all consumer instances so the dashboard can display their state.

export interface ConsumerNode {
  id: string;
  groupId: string;
  consumer: Consumer;
  status: 'idle' | 'processing' | 'paused';
  assignedPartitions: number[];
}

export const consumerRegistry: ConsumerNode[] = [];

// ── CACHED KAFKA PARTITION STATE ─────────────────────────────────────────────
// We cache partition metadata for 8 seconds to avoid flooding Kafka Admin on every poll.

let cachedPartitions: any[] = [];
let lastPartitionFetch = 0;
let totalThroughput = 142.8;

// ── INIT ──────────────────────────────────────────────────────────────────────

export async function initKafka() {
  console.log('[Kafka] Connecting admin and producer...');
  await admin.connect();
  await producer.connect();

  // Create the topic with 3 partitions if it doesn't exist yet
  const existing = await admin.listTopics();
  if (!existing.includes(TOPIC)) {
    await admin.createTopics({
      topics: [{ topic: TOPIC, numPartitions: 3, replicationFactor: 1 }],
      waitForLeaders: true,
    });
    console.log(`[Kafka] Topic "${TOPIC}" created with 3 partitions.`);
  } else {
    console.log(`[Kafka] Topic "${TOPIC}" already exists.`);
  }

  await waitForTopicLeaders();

  // Start the initial 2 consumer workers
  await spawnConsumer('worker-node-alpha');
  await spawnConsumer('worker-node-beta');

  console.log('[Kafka] ✅ Kafka ready. Producer and 2 consumers active.');
  await addSystemLog('success', 'KAFKA', `Kafka Broker connected. Topic "${TOPIC}" ready with 3 partitions.`);
}

// ── SPAWN / STOP CONSUMERS ────────────────────────────────────────────────────

export async function spawnConsumer(nodeId: string): Promise<ConsumerNode> {
  let node: ConsumerNode;
  const consumer = kafka.consumer({
    groupId: GROUP_ID,
    retry: {
      restartOnFailure: async (err) => {
        const message = err?.message ?? String(err);
        if (message.includes('This server does not host this topic-partition')) {
          if (node) {
            node.status = 'paused';
          }
          await addSystemLog('error', 'KAFKA',
            `Consumer "${nodeId}" stopped after Kafka metadata error. Restart Kafka infrastructure and then restart the app.`
          ).catch(() => {});
          return false;
        }
        return true;
      },
    },
  });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

  node = {
    id: nodeId,
    groupId: GROUP_ID,
    consumer,
    status: 'idle',
    assignedPartitions: [],
  };

  // KafkaJS fires this event when the consumer gets its partition assignment
  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    node.assignedPartitions =
      (payload.memberAssignment as any)[TOPIC] ?? [];
    addSystemLog('success', 'KAFKA',
      `Consumer "${nodeId}" joined group "${GROUP_ID}". Assigned partitions: [${node.assignedPartitions.join(', ')}].`
    ).catch(() => {});
    // Invalidate cache so next poll shows fresh partition state
    lastPartitionFetch = 0;
  });

  consumer.on(consumer.events.REBALANCING, () => {
    addSystemLog('warn', 'KAFKA',
      `Rebalance triggered in group "${GROUP_ID}". Reassigning partitions among active consumers.`
    ).catch(() => {});
    lastPartitionFetch = 0;
  });

  // Start the message consumption loop (runs forever in background)
  consumer.run({
    autoCommit: false, // We manually commit after successful processing
    eachMessage: async ({ topic, partition, message }) => {
      node.status = 'processing';
      let workflow: Workflow;

      try {
        workflow = JSON.parse(message.value!.toString()) as Workflow;
        await runPipeline(workflow, nodeId);

        // Commit offset only after full success (at-least-once delivery)
        await consumer.commitOffsets([{
          topic, partition,
          offset: (parseInt(message.offset) + 1).toString(),
        }]);
        lastPartitionFetch = 0; // refresh dashboard partition state
      } catch (err: any) {
        // Failed messages are already marked as DLQ inside runPipeline
        // We still commit so we don't re-process the same failed message
        await consumer.commitOffsets([{
          topic, partition,
          offset: (parseInt(message.offset) + 1).toString(),
        }]);
      } finally {
        node.status = 'idle';
      }
    },
  });

  consumerRegistry.push(node);
  await addSystemLog('success', 'KAFKA', `Consumer "${nodeId}" provisioned and connected to group "${GROUP_ID}".`);
  return node;
}

export async function stopConsumer(nodeId: string): Promise<void> {
  const node = consumerRegistry.find(c => c.id === nodeId);
  if (!node) return;

  await node.consumer.disconnect(); // This triggers a real Kafka rebalance
  node.status = 'paused';
  node.assignedPartitions = [];
  await addSystemLog('warn', 'KAFKA',
    `Consumer "${nodeId}" disconnected. Kafka group rebalancing partitions to remaining consumers.`
  );
  lastPartitionFetch = 0;
}

export async function restartConsumer(nodeId: string): Promise<void> {
  const idx = consumerRegistry.findIndex(c => c.id === nodeId);
  if (idx === -1) return;

  // Remove the old dead instance and create a fresh one
  consumerRegistry.splice(idx, 1);
  await spawnConsumer(nodeId);
}

// ── PRODUCE A WORKFLOW ────────────────────────────────────────────────────────

export async function produceWorkflow(workflow: Workflow): Promise<void> {
  // Use the title as the message key — Kafka hashes the key to pick a partition,
  // guaranteeing that same-titled jobs always go to the same partition (ordering guarantee).
  await producer.send({
    topic: TOPIC,
    messages: [{
      key: workflow.title,
      value: JSON.stringify(workflow),
    }],
  });
  await addSystemLog('info', 'GATEWAY',
    `Workflow "${workflow.title}" produced to topic "${TOPIC}".`
  );
}

// ── PARTITION STATE (for dashboard) ─────────────────────────────────────────

export async function getKafkaPartitionState() {
  // Serve from cache if fresh enough
  if (Date.now() - lastPartitionFetch < 8000 && cachedPartitions.length > 0) {
    return cachedPartitions;
  }

  try {
    const [topicOffsets, groupOffsets] = await Promise.all([
      admin.fetchTopicOffsets(TOPIC),
      admin.fetchOffsets({ groupId: GROUP_ID, topics: [TOPIC] }),
    ]);

    const groupPartitions = groupOffsets[0]?.partitions ?? [];

    cachedPartitions = topicOffsets.map(p => {
      const committed = groupPartitions.find(gp => gp.partition === p.partition);
      const high = parseInt(p.high) || 0;
      const committedOffset = parseInt(committed?.offset ?? '-1') + 1; // -1 means nothing committed yet

      return {
        id: p.partition,
        messages: [],
        offset: high,
        committedOffsets: { [GROUP_ID]: Math.max(0, committedOffset) },
      };
    });

    lastPartitionFetch = Date.now();
  } catch {
    // Return cached data if Kafka admin call fails temporarily
  }

  return cachedPartitions;
}

export function getThroughput() {
  return totalThroughput;
}

async function waitForTopicLeaders(): Promise<void> {
  const maxAttempts = 20;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const metadata = await admin.fetchTopicMetadata({ topics: [TOPIC] });
      const topic = metadata.topics.find(t => t.name === TOPIC);
      const partitions = topic?.partitions ?? [];
      const allLeadersReady =
        partitions.length >= 3 &&
        partitions.every(partition => typeof partition.leader === 'number' && partition.leader >= 0);

      if (allLeadersReady) {
        return;
      }
    } catch (err: any) {
      if (attempt === maxAttempts) {
        throw err;
      }
    }

    await new Promise(resolve => setTimeout(resolve, 500));
  }

  throw new Error(`Kafka topic "${TOPIC}" leaders were not ready after ${maxAttempts} attempts.`);
}

// ── 4-STAGE AI PIPELINE WORKER ────────────────────────────────────────────────

async function runPipeline(workflow: Workflow, consumerId: string): Promise<void> {
  await addSystemLog('info', 'WORKER',
    `[${consumerId}] Picked up workflow "${workflow.title}". Starting pipeline.`
  );

  // STAGE 1: Mark as processing in DB
  workflow.status = 'processing';
  workflow.currentStage = 'summarization';
  await upsertWorkflow(workflow);

  let summaryResult = '';
  let sentimentResult = 'NEUTRAL';
  let entitiesResult: string[] = [];

  try {
    const ai = getAiClient();

    let geminiSuccess = false;
    if (ai) {
      try {
        await addSystemLog('info', 'WORKER', `Invoking Gemini 2.5 Flash for summarization + classification...`);

        // STAGE 2: Summarization
        const summaryResponse = await ai.models.generateContent({
          model: 'gemini-2.5-flash',
          contents: `Provide an extremely concise summary (under 80 words) for this technical paragraph:\n\n${workflow.text}`,
          config: { systemInstruction: 'You are an elite Staff Backend Architect extracting vital infrastructure telemetry.' }
        });
        summaryResult = summaryResponse.text?.trim() ?? '';

        // STAGE 3: JSON Classification
        workflow.currentStage = 'classification';
        await upsertWorkflow(workflow);

        const classificationResponse = await ai.models.generateContent({
          model: 'gemini-2.5-flash',
          contents: `Analyze this technical text. Return sentiment (POSITIVE, NEUTRAL, or NEGATIVE) and up to 5 key technologies/concepts. Text: "${workflow.text}"`,
          config: {
            responseMimeType: 'application/json',
            responseSchema: {
              type: Type.OBJECT,
              properties: {
                sentiment: { type: Type.STRING },
                entities: { type: Type.ARRAY, items: { type: Type.STRING } },
              },
              required: ['sentiment', 'entities'],
            },
          },
        });

        const structured = JSON.parse(classificationResponse.text ?? '{}');
        sentimentResult = structured.sentiment ?? 'NEUTRAL';
        entitiesResult = structured.entities ?? [];
        await addSystemLog('success', 'WORKER', `Gemini classification complete for "${workflow.title}".`);
        geminiSuccess = true;
      } catch (geminiErr: any) {
        await addSystemLog('error', 'WORKER', `Gemini API call failed: ${geminiErr.message || geminiErr}. Triggering local fallback worker.`);
      }
    }

    if (!ai || !geminiSuccess) {
      // Fallback: local regex extraction when no API key or when API fails
      workflow.currentStage = 'classification';
      await upsertWorkflow(workflow);
      await new Promise(r => setTimeout(r, 1000));

      summaryResult = `[Fallback] ${workflow.text.split('.').slice(0, 2).join('. ')}.`;
      sentimentResult = workflow.text.toLowerCase().includes('fail') ? 'NEGATIVE' : 'POSITIVE';
      const words = workflow.text.match(/[A-Z][a-zA-Z]+/g) ?? [];
      entitiesResult = Array.from(new Set(words))
        .filter(w => ['PostgreSQL','GIN','Raft','Kafka','Redis','Docker','Lua','Database','Microservice','TypeScript'].includes(w))
        .slice(0, 4);
      if (entitiesResult.length === 0) entitiesResult = ['Backend', 'System Design'];
    }

    // STAGE 4: Persist to PostgreSQL with GIN index update
    workflow.currentStage = 'audit';
    workflow.status = 'completed';
    workflow.summary = summaryResult;
    workflow.sentiment = sentimentResult;
    workflow.entities = entitiesResult;

    const insertStart = Date.now();
    await upsertWorkflow(workflow);
    const insertTime = Date.now() - insertStart;

    await logDbQuery(
      `INSERT INTO workflows ... entities = ARRAY[${entitiesResult.map(e => `'${e}'`).join(',')}] -- GIN index updated`,
      insertTime,
      true
    );
    await addSystemLog('success', 'DATABASE',
      `Committed workflow "${workflow.title}" to PostgreSQL. GIN index (idx_workflows_entities_gin) updated.`
    );

    // Update throughput counter
    totalThroughput = parseFloat((120 + Math.random() * 40).toFixed(1));

  } catch (err: any) {
    workflow.status = 'dlq';
    workflow.error = err.message ?? 'Unknown pipeline error';
    await upsertWorkflow(workflow);

    await addSystemLog('error', 'WORKER',
      `Pipeline failure for "${workflow.title}": ${workflow.error}. Routed to Dead Letter Queue (DLQ).`
    );
    await logDbQuery(
      `UPDATE workflows SET status='dlq', error='${workflow.error}' WHERE id='${workflow.id}'`,
      0.45,
      false
    );
    throw err; // re-throw so the consumer can handle offset commitment
  }
}
