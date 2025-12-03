import axios from 'axios';
import pLimit from 'p-limit';
import { initDb } from './db.js';
import { log } from './logger.js';
import { scrapeParts } from './scraper/realoem.js';
import { scrapeDiagram } from './scraper/bimmerApi.js';
import { config } from './config.js';

const limit = pLimit(config.maxConcurrency);

async function safeGet(url) {
  try {
    const { data } = await axios.get(url);
    return data;
  } catch (error) {
    log.warn('Request failed', url, error.message);
    return null;
  }
}

async function getModels() {
  const data = await safeGet('https://bimmer.work/api/models?brand=BMW&market=EU');
  if (!data || !Array.isArray(data.models)) return [];
  return data.models;
}

async function getModifications(model) {
  const data = await safeGet(`https://bimmer.work/api/modifications?model=${model}`);
  if (!data || !Array.isArray(data.modifications)) return [];
  return data.modifications;
}

async function getGroups(modification) {
  const data = await safeGet(`https://bimmer.work/api/groups?mod=${modification}`);
  if (!data || !Array.isArray(data.groups)) return [];
  return data.groups;
}

async function getSubgroups(modification, group) {
  const data = await safeGet(`https://bimmer.work/api/subgroups?mod=${modification}&group=${group}`);
  if (!data || !Array.isArray(data.subgroups)) return [];
  return data.subgroups;
}

async function processSubgroup(modificationId, groupId, subgroup) {
  const assemblyId = subgroup.assemblyId || null;
  await scrapeParts(modificationId, groupId, subgroup.id, assemblyId);
  await scrapeDiagram(modificationId, subgroup.id, assemblyId);
}

async function processGroup(modificationId, groupId) {
  const subgroups = await getSubgroups(modificationId, groupId);
  const tasks = subgroups.map((sub) => limit(() => processSubgroup(modificationId, groupId, sub)));
  await Promise.all(tasks);
}

async function processModification(modification) {
  const groups = await getGroups(modification.id || modification);
  for (const group of groups) {
    await processGroup(modification.id || modification, group.id || group);
  }
}

async function run() {
  await initDb();
  const models = await getModels();
  for (const model of models) {
    log.info('Processing model', model.code || model);
    const modifications = await getModifications(model.code || model);
    const tasks = modifications.map((mod) => limit(() => processModification(mod)));
    await Promise.all(tasks);
  }
  log.info('Runner finished');
}

if (process.argv[1] && process.argv[1].includes('runner.js')) {
  run().catch((error) => {
    log.error('Runner failed', error);
    process.exit(1);
  });
}
