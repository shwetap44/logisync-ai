/**
 * Database initialisation script.
 * Run with: npm run db:init
 * This creates the "logisync" database if it doesn't exist,
 * and sets up all tables and indexes from schema.sql.
 */
import { Client } from 'pg';
import { pool } from './client';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

dotenv.config();
dotenv.config({ path: '.env.local' });

const __dirname = path.dirname(fileURLToPath(import.meta.url));

async function init() {
  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) {
    console.error('DATABASE_URL is not set.');
    process.exit(1);
  }

  // Parse host/port/credentials from database URL to connect to the default "postgres" db first
  const urlParts = new URL(dbUrl);
  const targetDb = urlParts.pathname.substring(1) || 'logisync';
  
  // Create a connection URL for the default "postgres" database
  urlParts.pathname = '/postgres';
  const defaultDbUrl = urlParts.toString();

  console.log(`[DB Init] Connecting to default database to verify "${targetDb}" exists...`);
  const client = new Client({ connectionString: defaultDbUrl });
  
  try {
    await client.connect();
    
    // Check if the target database exists
    const res = await client.query(`SELECT 1 FROM pg_database WHERE datname = $1`, [targetDb]);
    
    if (res.rowCount === 0) {
      console.log(`[DB Init] Database "${targetDb}" does not exist. Creating it...`);
      // CREATE DATABASE cannot run inside a transaction block, so we run it on the raw client
      await client.query(`CREATE DATABASE ${targetDb}`);
      console.log(`[DB Init] Database "${targetDb}" created successfully.`);
    } else {
      console.log(`[DB Init] Database "${targetDb}" already exists.`);
    }
  } catch (err: any) {
    console.error('[DB Init] ❌ Error checking/creating database:', err.message);
    process.exit(1);
  } finally {
    await client.end();
  }

  console.log(`[DB Init] Connecting to database "${targetDb}" to run schema...`);
  const dbClient = await pool.connect();

  try {
    const schemaPath = path.resolve(__dirname, 'schema.sql');
    const sql = fs.readFileSync(schemaPath, 'utf-8');

    console.log('[DB Init] Running schema.sql...');
    await dbClient.query(sql);
    console.log('[DB Init] ✅ All tables, indexes, and seed data created successfully!');
  } catch (err: any) {
    console.error('[DB Init] ❌ Error running schema:', err.message);
    process.exit(1);
  } finally {
    dbClient.release();
    await pool.end();
  }
}

init();
