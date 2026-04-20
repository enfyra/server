process.env.LOG_DISABLE_FILES = '1';
process.env.LOG_DISABLE_CONSOLE = '1';

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { Logger, __pinoInstanceForTests } from '../../src/shared/logger';
import { logStore } from '../../src/shared/log-store';

const pinoInstance = __pinoInstanceForTests();

type LogCall = { level: string; msg: string; meta: Record<string, any> };

function captureCalls(): { calls: LogCall[]; restore: () => void } {
  const calls: LogCall[] = [];
  const spies = (['info', 'warn', 'error', 'debug', 'trace'] as const).map((lvl) =>
    vi.spyOn(pinoInstance, lvl).mockImplementation(((a: any, b?: any) => {
      const meta = typeof a === 'object' && a !== null ? a : {};
      const msg = typeof a === 'string' ? a : (typeof b === 'string' ? b : '');
      calls.push({ level: lvl, msg, meta });
      return pinoInstance as any;
    }) as any),
  );
  return {
    calls,
    restore: () => spies.forEach((s) => s.mockRestore()),
  };
}

describe('Logger — basic string messages', () => {
  let cap: ReturnType<typeof captureCalls>;
  beforeEach(() => { cap = captureCalls(); });
  afterEach(() => cap.restore());

  it('log(str) → pino.info with message and context', () => {
    const logger = new Logger('Svc');
    logger.log('started');
    expect(cap.calls).toHaveLength(1);
    expect(cap.calls[0].level).toBe('info');
    expect(cap.calls[0].msg).toBe('started');
    expect(cap.calls[0].meta.context).toBe('Svc');
  });

  it('log(str, ctxOverride) — override trumps constructor context', () => {
    const logger = new Logger('Svc');
    logger.log('x', 'Override');
    expect(cap.calls[0].meta.context).toBe('Override');
  });

  it('error(str, err) — Error trace becomes stack', () => {
    const logger = new Logger('Svc');
    const err = new Error('boom');
    logger.error('failed', err);
    expect(cap.calls[0].level).toBe('error');
    expect(cap.calls[0].msg).toBe('failed');
    expect(cap.calls[0].meta.stack).toContain('Error: boom');
  });

  it('error(str, stringTrace)', () => {
    const logger = new Logger('Svc');
    logger.error('failed', 'trace-text');
    expect(cap.calls[0].meta.stack).toBe('trace-text');
  });

  it('warn/debug/verbose route to warn/debug/trace levels', () => {
    const logger = new Logger('X');
    logger.warn('w');
    logger.debug('d');
    logger.verbose('v');
    expect(cap.calls.map((c) => c.level)).toEqual(['warn', 'debug', 'trace']);
  });

  it('numbers, booleans, null are stringified', () => {
    const logger = new Logger('X');
    logger.log(42);
    logger.log(false);
    logger.log(null);
    expect(cap.calls.map((c) => c.msg)).toEqual(['42', 'false', 'null']);
  });
});

describe('Logger — object message dedup', () => {
  let cap: ReturnType<typeof captureCalls>;
  beforeEach(() => { cap = captureCalls(); });
  afterEach(() => cap.restore());

  it('object with message → msg extracted, no duplicate message key in meta', () => {
    const logger = new Logger();
    logger.log({ message: 'Hello', userId: 'u1', ttl: 30 }, 'Ctx');
    expect(cap.calls[0].msg).toBe('Hello');
    expect(cap.calls[0].meta).not.toHaveProperty('message');
    expect(cap.calls[0].meta.userId).toBe('u1');
    expect(cap.calls[0].meta.ttl).toBe(30);
    expect(cap.calls[0].meta.context).toBe('Ctx');
  });

  it('object missing message → uses level-specific fallback', () => {
    const logger = new Logger();
    logger.log({ foo: 1 }, 'C');
    logger.error({ stack: 's' }, undefined, 'C');
    logger.warn({ a: 1 }, 'C');
    logger.debug({ d: 1 }, 'C');
    logger.verbose({ v: 1 }, 'C');
    expect(cap.calls.map((c) => c.msg)).toEqual(['Log', 'Error', 'Warning', 'Debug', 'Verbose']);
  });

  it('object with falsy message falls back to default', () => {
    const logger = new Logger();
    logger.log({ message: '', x: 1 });
    logger.warn({ message: null, y: 2 });
    expect(cap.calls[0].msg).toBe('Log');
    expect(cap.calls[1].msg).toBe('Warning');
  });

  it('Error instance as message → uses .message, stack stored in meta', () => {
    const logger = new Logger();
    const err = new Error('Kaboom');
    logger.error(err);
    expect(cap.calls[0].msg).toBe('Kaboom');
    expect(cap.calls[0].meta.stack).toContain('Error: Kaboom');
  });
});

describe('Logger — object trace parameter', () => {
  let cap: ReturnType<typeof captureCalls>;
  beforeEach(() => { cap = captureCalls(); });
  afterEach(() => cap.restore());

  it('error(str, { message, ...rest }) → message stripped from meta', () => {
    const logger = new Logger('X');
    logger.error('Broke', { message: 'ignored', statusCode: 500, url: '/a' } as any);
    expect(cap.calls[0].msg).toBe('Broke');
    expect(cap.calls[0].meta).not.toHaveProperty('message');
    expect(cap.calls[0].meta.statusCode).toBe(500);
    expect(cap.calls[0].meta.url).toBe('/a');
  });
});

describe('Logger — correlationId auto-inject from ALS', () => {
  let cap: ReturnType<typeof captureCalls>;
  beforeEach(() => { cap = captureCalls(); });
  afterEach(() => cap.restore());

  it('logs inside logStore.run carry correlationId via pino mixin', () => {
    const logger = new Logger('Req');
    logStore.run({ correlationId: 'req_abc', context: { userId: 'u1' } }, () => {
      logger.log('inside');
    });
    const last = cap.calls[0].meta;
    expect(last.context).toBe('Req');
  });

  it('logs outside ALS → no correlationId in meta', () => {
    const logger = new Logger('Req');
    logger.log('outside');
    expect(cap.calls[0].meta.correlationId).toBeUndefined();
  });
});

describe('Logger — fatal shorthand', () => {
  let cap: ReturnType<typeof captureCalls>;
  beforeEach(() => { cap = captureCalls(); });
  afterEach(() => cap.restore());

  it('fatal() emits error with fatal: true', () => {
    const logger = new Logger('Sys');
    logger.fatal('OOM');
    expect(cap.calls[0].level).toBe('error');
    expect(cap.calls[0].meta.fatal).toBe(true);
  });
});
