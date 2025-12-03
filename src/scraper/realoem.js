import axios from 'axios';
import cheerio from 'cheerio';
import { config } from '../config.js';
import { log } from '../logger.js';
import { savePart } from '../db.js';
import { fallback7Zap } from './sevenZap.js';

async function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithRetry(url, attempts = config.retryAttempts) {
  for (let i = 0; i < attempts; i += 1) {
    try {
      const response = await axios.get(url);
      return response.data;
    } catch (error) {
      log.warn(`Request failed (${i + 1}/${attempts}) for ${url}:`, error.message);
      if (i === attempts - 1) throw error;
      await delay(config.retryDelayMs);
    }
  }
  return null;
}

export async function scrapeParts(modificationId, group, subgroup, assemblyId = null) {
  const url = `https://www.realoem.com/bmw/enUS/showparts?id=${modificationId}&group=${group}&hg=${subgroup}`;
  try {
    const html = await fetchWithRetry(url);
    const $ = cheerio.load(html);
    const rows = $('table.parts > tr.part');

    if (!rows.length) {
      log.warn('No parts found on Realoem, falling back to 7zap');
      return fallback7Zap(modificationId, group, assemblyId);
    }

    for (const row of rows.toArray()) {
      const oem = $(row).find('td.partno').text().trim();
      const name = $(row).find('td.name').text().trim();
      const qty = $(row).find('td.qty').text().trim();
      const note = $(row).find('td.remark').text().trim();

      log.info('Parsed OEM', oem);
      await savePart({ assembly_id: assemblyId, oem, name, qty, note });
    }
  } catch (error) {
    log.error('Failed to parse Realoem, using 7zap fallback', error.message);
    await fallback7Zap(modificationId, group, assemblyId);
  }
}
