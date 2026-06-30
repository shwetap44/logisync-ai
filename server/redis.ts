/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 *
 * Redis Sliding Window Rate Limiter — using real Redis sorted sets.
 *
 * How it works (real production technique used at Stripe, GitHub, etc.):
 *   Redis Sorted Set key = "ratelimit:{ip}"
 *   Each member = a unique request ID
 *   Each score  = the Unix timestamp of the request (in ms)
 *
 *   On every request:
 *   1. ZREMRANGEBYSCORE key 0 (now - window)  → evict stale entries
 *   2. ZCARD key                               → count active requests
 *   3. If count < limit → ZADD + allow
 *      If count >= limit → block with HTTP 429
 *   4. EXPIRE key (auto-cleanup when window ends)
 */
import Redis from 'ioredis';
import crypto from 'crypto';

const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';

export const redis = new Redis(redisUrl, {
  lazyConnect: true,
  retryStrategy: (times) => Math.min(times * 200, 3000),
});

redis.on('connect', () => console.log('[Redis] Connected to Redis server.'));
redis.on('error', (err) => console.error('[Redis] Connection error:', err.message));

export async function isRateLimited(
  ip: string,
  limit: number = 6,
  windowMs: number = 20000
): Promise<{ limited: boolean; remaining: number }> {
  const key = `logisync:ratelimit:${ip}`;
  const now = Date.now();
  const windowStart = now - windowMs;

  // Use a pipeline to send all commands to Redis in one round-trip (atomic-ish)
  const pipeline = redis.pipeline();
  pipeline.zremrangebyscore(key, 0, windowStart); // 1. Evict old entries
  pipeline.zcard(key);                            // 2. Count active entries
  pipeline.expire(key, Math.ceil(windowMs / 1000) + 1); // 3. Auto-expire key

  const results = await pipeline.exec();
  const currentCount = (results![1][1] as number) ?? 0;

  if (currentCount >= limit) {
    return { limited: true, remaining: 0 };
  }

  // Allow the request — record it with current timestamp as the score
  await redis.zadd(key, now, `${now}-${crypto.randomUUID()}`);

  return { limited: false, remaining: limit - currentCount - 1 };
}
