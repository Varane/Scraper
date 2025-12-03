import express from 'express';
import { initDb } from './db.js';
import { findAssembly, findPartByOem, findPartsByModel } from './db.js';
import { log } from './logger.js';

const app = express();
app.use(express.json());

app.get('/oem/:oem', async (req, res) => {
  try {
    const result = await findPartByOem(req.params.oem);
    if (!result.part) return res.status(404).json({ message: 'Not found' });
    return res.json({
      name: result.part.name,
      photo: null,
      compatibility: result.compatibility,
      assemblies: [result.part.assembly_id].filter(Boolean),
      part: result.part,
    });
  } catch (error) {
    log.error('Error fetching OEM', error);
    return res.status(500).json({ error: 'Internal error' });
  }
});

app.get('/model/:id/parts', async (req, res) => {
  try {
    const parts = await findPartsByModel(req.params.id);
    return res.json(parts);
  } catch (error) {
    log.error('Error fetching model parts', error);
    return res.status(500).json({ error: 'Internal error' });
  }
});

app.get('/assembly/:id', async (req, res) => {
  try {
    const assembly = await findAssembly(req.params.id);
    return res.json(assembly);
  } catch (error) {
    log.error('Error fetching assembly', error);
    return res.status(500).json({ error: 'Internal error' });
  }
});

const port = process.env.PORT || 3000;

async function start() {
  await initDb();
  app.listen(port, () => log.info(`API listening on ${port}`));
}

start().catch((error) => {
  log.error('Failed to start server', error);
  process.exit(1);
});
