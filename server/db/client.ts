/**
 * PostgreSQL connection pool.
 * Uses a pool (not a single client) so multiple concurrent requests
 * can each get a connection without waiting for each other.
 */
import { Pool } from 'pg';
import dotenv from 'dotenv';

// Load env variables
dotenv.config();
dotenv.config({ path: '.env.local' });

if (!process.env.DATABASE_URL) {
  throw new Error('DATABASE_URL environment variable is not set.');
}

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,                // max 10 concurrent connections in the pool
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

pool.on('error', (err) => {
  console.error('[PostgreSQL] Unexpected client error:', err.message);
});
