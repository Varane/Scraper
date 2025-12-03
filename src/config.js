export const config = {
  databaseUrl: process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost:5432/parts',
  maxConcurrency: 3,
  retryAttempts: 3,
  retryDelayMs: 1500,
};
