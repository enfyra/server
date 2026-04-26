import { describe, it, expect, beforeEach, afterAll } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import pino from 'pino';
import { LogReaderService } from 'src/modules/admin';

function makeTempLogDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'enfyra-log-'));
  return dir;
}

function writePinoLine(
  filePath: string,
  logger: pino.Logger,
  fn: () => void,
): void {
  fn();
  logger.flush?.();
}

describe('LogReaderService — compat with pino JSON output', () => {
  const originalCwd = process.cwd();
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = makeTempLogDir();
    fs.mkdirSync(path.join(tmpDir, 'logs'), { recursive: true });
    process.chdir(tmpDir);
  });

  afterAll(() => process.chdir(originalCwd));

  it('parseLogLine extracts id/timestamp/level/context/correlationId/message from pino line', () => {
    const file = path.join(tmpDir, 'logs', 'app-2026-04-19.log');
    const dest = pino.destination({ dest: file, sync: true });
    const logger = pino(
      {
        messageKey: 'message',
        base: { service: 'enfyra-server' },
        formatters: { level: (label: string) => ({ level: label }) },
        timestamp: () => `,"timestamp":"${new Date().toISOString()}"`,
        mixin: () => ({ id: 'log_test_0001', correlationId: 'req_xyz' }),
      },
      dest,
    );
    writePinoLine(file, logger, () => {
      logger.info({ context: 'Svc', userId: 'u1' }, 'Hello world');
    });

    const reader = new LogReaderService();
    const content = fs
      .readFileSync(file, 'utf-8')
      .trim()
      .split('\n')
      .filter(Boolean);
    expect(content.length).toBeGreaterThan(0);
    const parsed = (reader as any).parseLogLine(content[0]);
    expect(parsed.level).toBe('info');
    expect(parsed.message).toBe('Hello world');
    expect(parsed.id).toBe('log_test_0001');
    expect(parsed.correlationId).toBe('req_xyz');
    expect(parsed.context).toBe('Svc');
  });

  it('matchesFilter respects correlationId match', () => {
    const file = path.join(tmpDir, 'logs', 'app-2026-04-19.log');
    const dest = pino.destination({ dest: file, sync: true });
    const logger = pino(
      {
        messageKey: 'message',
        formatters: { level: (label: string) => ({ level: label }) },
        timestamp: () => `,"timestamp":"${new Date().toISOString()}"`,
      },
      dest,
    );
    logger.info({ correlationId: 'req_a', context: 'X' }, 'line-a');
    logger.info({ correlationId: 'req_b', context: 'Y' }, 'line-b');

    const reader = new LogReaderService();
    const lines = fs
      .readFileSync(file, 'utf-8')
      .trim()
      .split('\n')
      .filter(Boolean);
    const match = reader as any;
    expect(
      match.matchesFilter(lines[0], undefined, undefined, undefined, 'req_a'),
    ).toBe(true);
    expect(
      match.matchesFilter(lines[1], undefined, undefined, undefined, 'req_a'),
    ).toBe(false);
  });

  it('matchesFilter respects level match', () => {
    const file = path.join(tmpDir, 'logs', 'app-2026-04-19.log');
    const dest = pino.destination({ dest: file, sync: true });
    const logger = pino(
      {
        messageKey: 'message',
        formatters: { level: (label: string) => ({ level: label }) },
      },
      dest,
    );
    logger.info({}, 'info-line');
    logger.error({}, 'error-line');
    const reader = new LogReaderService();
    const lines = fs
      .readFileSync(file, 'utf-8')
      .trim()
      .split('\n')
      .filter(Boolean);
    const m = reader as any;
    expect(m.matchesFilter(lines[0], undefined, 'info')).toBe(true);
    expect(m.matchesFilter(lines[0], undefined, 'error')).toBe(false);
    expect(m.matchesFilter(lines[1], undefined, 'error')).toBe(true);
  });

  it('getLogFiles lists .log files, skipping dotfiles and pm2-', () => {
    const logs = path.join(tmpDir, 'logs');
    fs.writeFileSync(path.join(logs, 'app-2026-04-19.log'), '');
    fs.writeFileSync(path.join(logs, 'error-2026-04-19.log'), '');
    fs.writeFileSync(path.join(logs, '.hidden.log'), '');
    fs.writeFileSync(path.join(logs, 'pm2-x.log'), '');
    fs.writeFileSync(path.join(logs, 'readme.txt'), '');

    const reader = new LogReaderService();
    const files = reader.getLogFiles();
    const names = files.map((f) => f.name).sort();
    expect(names).toEqual(['app-2026-04-19.log', 'error-2026-04-19.log']);
  });

  it('tailLog returns parsed entries newest-first', async () => {
    const file = path.join(tmpDir, 'logs', 'app-2026-04-19.log');
    const dest = pino.destination({ dest: file, sync: true });
    const logger = pino(
      {
        messageKey: 'message',
        formatters: { level: (label: string) => ({ level: label }) },
      },
      dest,
    );
    logger.info({}, 'one');
    logger.info({}, 'two');
    logger.info({}, 'three');

    const reader = new LogReaderService();
    const tail = reader.tailLog('app-2026-04-19.log', 3) as any;
    expect(tail.lines.map((l: any) => l.message)).toEqual([
      'three',
      'two',
      'one',
    ]);
  });
});
