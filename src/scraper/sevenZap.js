import axios from 'axios';
import cheerio from 'cheerio';
import { log } from '../logger.js';
import { savePart } from '../db.js';

export async function fallback7Zap(model, group, assemblyId = null) {
  const url = `https://7zap.com/en/catalog/bmw/part-group/?model=${model}&group=${group}`;
  try {
    const { data } = await axios.get(url);
    const $ = cheerio.load(data);
    const rows = $('table tr');

    for (const row of rows.toArray()) {
      const cells = $(row).find('td');
      if (cells.length < 4) continue;
      const oem = $(cells[1]).text().trim();
      const name = $(cells[2]).text().trim();
      const qty = $(cells[3]).text().trim();

      if (!oem) continue;
      log.info('Fallback OEM', oem);
      await savePart({ assembly_id: assemblyId, oem, name, qty, note: null });
    }
  } catch (error) {
    log.error('7zap fallback failed', error.message);
  }
}
