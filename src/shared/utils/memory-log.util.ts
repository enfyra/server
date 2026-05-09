import { Logger } from '../logger';

type MemoryLogMeta = Record<string, unknown>;

function toMb(bytes: number): number {
  return Math.round((bytes / 1024 / 1024) * 10) / 10;
}

export function createMemorySnapshot(): Record<string, number> {
  const memory = process.memoryUsage();
  return {
    rssMb: toMb(memory.rss),
    heapUsedMb: toMb(memory.heapUsed),
    heapTotalMb: toMb(memory.heapTotal),
    externalMb: toMb(memory.external),
    arrayBuffersMb: toMb(memory.arrayBuffers),
    pid: process.pid,
    uptimeSec: Math.round(process.uptime()),
  };
}

export function logMemory(
  logger: Logger,
  label: string,
  meta: MemoryLogMeta = {},
): void {
  if (process.env.MEMORY_LOG !== '1') return;

  logger.log({
    message: `[memory] ${label}`,
    memory: createMemorySnapshot(),
    ...meta,
  });
}
