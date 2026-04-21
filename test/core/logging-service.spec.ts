import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { LoggingService } from '../../src/core/exceptions/services/logging.service';
import { logStore } from '../../src/shared/log-store';
import { __pinoInstanceForTests } from '../../src/shared/logger';

const pino = __pinoInstanceForTests();

type Call = { level: string; msg: string; meta: Record<string, any> };

function capture(): { calls: Call[]; restore: () => void } {
  const calls: Call[] = [];
  const spies = (['info', 'warn', 'error', 'debug', 'trace'] as const).map(
    (lvl) =>
      vi.spyOn(pino, lvl).mockImplementation(((a: any, b?: any) => {
        const meta = typeof a === 'object' && a !== null ? a : {};
        const msg = typeof a === 'string' ? a : typeof b === 'string' ? b : '';
        calls.push({ level: lvl, msg, meta });
        return pino as any;
      }) as any),
  );
  return { calls, restore: () => spies.forEach((s) => s.mockRestore()) };
}

describe('LoggingService — ALS correlation', () => {
  let cap: ReturnType<typeof capture>;
  beforeEach(() => {
    cap = capture();
  });
  afterEach(() => cap.restore());

  it('run() creates isolated ALS frame with fresh correlationId slot', () => {
    const svc = new LoggingService();
    svc.run(() => {
      svc.setCorrelationId('req_1');
      svc.log('inside');
      expect(logStore.getStore()?.correlationId).toBe('req_1');
    });
    expect(logStore.getStore()).toBeUndefined();
  });

  it('setContext merges keys into ALS context', () => {
    const svc = new LoggingService();
    svc.run(() => {
      svc.setContext({ userId: 'u1', ip: '1.2.3.4' });
      expect(logStore.getStore()?.context).toEqual({
        userId: 'u1',
        ip: '1.2.3.4',
      });
      svc.setContext({ method: 'POST' });
      expect(logStore.getStore()?.context).toEqual({
        userId: 'u1',
        ip: '1.2.3.4',
        method: 'POST',
      });
    });
  });

  it('clearContext empties correlationId + context', () => {
    const svc = new LoggingService();
    svc.run(() => {
      svc.setCorrelationId('x');
      svc.setContext({ a: 1 });
      svc.clearContext();
      expect(logStore.getStore()?.correlationId).toBeUndefined();
      expect(logStore.getStore()?.context).toEqual({});
    });
  });

  it('setContext with correlationId key routes to setCorrelationId', () => {
    const svc = new LoggingService();
    svc.run(() => {
      svc.setContext({ correlationId: 'from-ctx', userId: 'u1' });
      expect(logStore.getStore()?.correlationId).toBe('from-ctx');
      expect(logStore.getStore()?.context).toEqual({ userId: 'u1' });
    });
  });
});

describe('LoggingService — structured event methods', () => {
  let cap: ReturnType<typeof capture>;
  beforeEach(() => {
    cap = capture();
  });
  afterEach(() => cap.restore());

  it('logResponse emits API Response with method/url/statusCode/responseTime', () => {
    const svc = new LoggingService();
    svc.logResponse('POST', '/api/x', 201, 42, 'u1', { extra: true });
    expect(cap.calls).toHaveLength(1);
    expect(cap.calls[0].level).toBe('info');
    expect(cap.calls[0].msg).toBe('API Response');
    expect(cap.calls[0].meta.data.method).toBe('POST');
    expect(cap.calls[0].meta.data.statusCode).toBe(201);
    expect(cap.calls[0].meta.data.responseTime).toBe('42ms');
    expect(cap.calls[0].meta.data.userId).toBe('u1');
  });

  it('logDatabaseOperation success → info, failure → error', () => {
    const svc = new LoggingService();
    svc.logDatabaseOperation('insert', 't', 10, true);
    svc.logDatabaseOperation('insert', 't', 10, false, new Error('boom'));
    expect(cap.calls[0].level).toBe('info');
    expect(cap.calls[0].msg).toBe('Database Operation');
    expect(cap.calls[1].level).toBe('error');
    expect(cap.calls[1].msg).toBe('Database Operation Failed');
  });

  it('logScriptExecution with error attaches error to data', () => {
    const svc = new LoggingService();
    svc.logScriptExecution('script_1', 5, false, 'syntax error');
    expect(cap.calls[0].meta.data.scriptId).toBe('script_1');
    expect(cap.calls[0].meta.data.error).toBe('syntax error');
  });

  it('logSecurityEvent emits warn level', () => {
    const svc = new LoggingService();
    svc.logSecurityEvent('brute_force', 'u1', '1.2.3.4', { attempts: 10 });
    expect(cap.calls[0].level).toBe('warn');
    expect(cap.calls[0].meta.data.event).toBe('brute_force');
    expect(cap.calls[0].meta.data.attempts).toBe(10);
  });

  it('logBusinessEvent emits info level with entity info', () => {
    const svc = new LoggingService();
    svc.logBusinessEvent('order_placed', 'u1', 'order', 'o_1', { total: 99 });
    expect(cap.calls[0].level).toBe('info');
    expect(cap.calls[0].meta.data.entity).toBe('order');
    expect(cap.calls[0].meta.data.entityId).toBe('o_1');
    expect(cap.calls[0].meta.data.total).toBe(99);
  });
});
