/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

export interface Workflow {
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

export interface KafkaMessage {
  id: string;
  topic: string;
  partition: number;
  offset: number;
  payload: any;
  timestamp: number;
  retries: number;
}

export interface KafkaPartition {
  id: number;
  messages: KafkaMessage[];
  offset: number;
  committedOffsets: Record<string, number>; // consumerGroup -> offset
}

export interface Consumer {
  id: string;
  groupId: string;
  assignedPartitions: number[];
  status: 'idle' | 'processing' | 'paused';
  currentMessageId?: string;
}

export interface RedisLimitTracker {
  timestamps: number[];
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
