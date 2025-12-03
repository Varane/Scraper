import pg from 'pg';
import { config } from './config.js';
import { log } from './logger.js';

const { Pool } = pg;

export const pool = new Pool({ connectionString: config.databaseUrl });

export async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS parts (
      id SERIAL PRIMARY KEY,
      assembly_id INT,
      oem TEXT,
      name TEXT,
      qty INT,
      note TEXT
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS compatibility (
      id SERIAL PRIMARY KEY,
      oem TEXT,
      model TEXT,
      modification TEXT
    );
  `);

  log.info('Database tables ensured');
}

export async function savePart(part) {
  const { assembly_id, oem, name, qty, note } = part;
  const result = await pool.query(
    `INSERT INTO parts (assembly_id, oem, name, qty, note)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (id) DO NOTHING
     RETURNING *;`,
    [assembly_id || null, oem, name, qty ? Number(qty) : null, note || null]
  );
  return result.rows[0];
}

export async function saveCompatibility(oem, models = [], modification = null) {
  for (const model of models) {
    await pool.query(
      `INSERT INTO compatibility (oem, model, modification)
       VALUES ($1, $2, $3);`,
      [oem, model, modification]
    );
  }
}

export async function findPartByOem(oem) {
  const part = await pool.query('SELECT * FROM parts WHERE oem = $1', [oem]);
  const compatibility = await pool.query('SELECT model, modification FROM compatibility WHERE oem = $1', [oem]);
  return { part: part.rows[0], compatibility: compatibility.rows };
}

export async function findPartsByModel(model) {
  const results = await pool.query(
    `SELECT p.* FROM parts p
     JOIN compatibility c ON c.oem = p.oem
     WHERE c.model = $1`,
    [model]
  );
  return results.rows;
}

export async function findAssembly(id) {
  const parts = await pool.query('SELECT * FROM parts WHERE assembly_id = $1', [id]);
  return { id: Number(id), parts: parts.rows, children: [] };
}
