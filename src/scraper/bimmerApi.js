import axios from 'axios';
import { saveCompatibility, savePart } from '../db.js';
import { log } from '../logger.js';
import { config } from '../config.js';

async function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithRetry(url, attempts = config.retryAttempts) {
  for (let i = 0; i < attempts; i += 1) {
    try {
      const response = await axios.get(url);
      return response.data;
    } catch (error) {
      log.warn(`Bimmer API request failed (${i + 1}/${attempts})`, error.message);
      if (i === attempts - 1) throw error;
      await delay(config.retryDelayMs);
    }
  }
  return null;
}

export async function scrapeDiagram(mod, grp, assemblyId = null) {
  const url = `https://bimmer.work/api/diagram?id=${mod}&group=${grp}&svg=1`;
  const data = await fetchWithRetry(url);
  if (!data || !Array.isArray(data.parts)) return;

  for (const part of data.parts) {
    log.info('Diagram OEM', part.number);
    await savePart({
      assembly_id: assemblyId,
      oem: part.number,
      name: part.name,
      qty: part.qty,
      note: part.note || null,
    });
    if (part.models) {
      await saveCompatibility(part.number, part.models, mod);
    }
  }
}
