import { Queue, type ConnectionOptions } from 'bullmq';
import { env } from '../env';

export function buildQueueConnectionOptions(redisUri: string): ConnectionOptions {
  const parsed = new URL(redisUri);
  const port =
    parsed.port.length > 0
      ? Number(parsed.port)
      : parsed.protocol === 'rediss:'
        ? 6380
        : 6379;
  const dbPath = parsed.pathname.replace(/^\//, '');
  const db = dbPath.length > 0 ? Number(dbPath) : undefined;
  const options: ConnectionOptions = {
    host: parsed.hostname,
    port,
  };

  if (parsed.username) {
    options.username = decodeURIComponent(parsed.username);
  }
  if (parsed.password) {
    options.password = decodeURIComponent(parsed.password);
  }
  if (db !== undefined && Number.isFinite(db)) {
    options.db = db;
  }
  if (parsed.protocol === 'rediss:') {
    options.tls = {};
  }

  return options;
}

export function createRuntimeQueue(name: string): Queue {
  return new Queue(name, {
    prefix: env.NODE_NAME,
    connection: buildQueueConnectionOptions(env.REDIS_URI),
  });
}

export async function closeRuntimeQueue(queue: Queue): Promise<void> {
  await queue.close();
}
