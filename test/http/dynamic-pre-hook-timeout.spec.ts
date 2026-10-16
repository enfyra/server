import { EventEmitter } from 'node:events';
import { describe, expect, it, vi } from 'vitest';
import { dynamicInterceptorBegin } from '../../src/http/middlewares/dynamic-interceptor.middleware';

describe('dynamic pre-hook method deadline', () => {
  for (const timeout of [600000, 30000, undefined]) {
    it(`uses the selected method budget ${timeout}`, async () => {
      const executor = {
        register: vi.fn(),
        runBatch: vi.fn(async () => ({ shortCircuit: false })),
      };
      const req = Object.assign(new EventEmitter(), {
        method: 'POST',
        path: '/v2/embeddings',
        headers: {},
        routeData: {
          routeMethodConfig: { timeout },
          context: { $share: { $logs: [] } },
          preHooks: [{ code: 'return undefined;' }],
          postHooks: [],
        },
      });
      const res = Object.assign(new EventEmitter(), {
        statusCode: 200,
        writableEnded: false,
        destroyed: false,
        json: vi.fn(),
      });
      const next = vi.fn();
      await dynamicInterceptorBegin(executor as any)(req, res as any, next);
      const [, remainingMs, options] = executor.runBatch.mock
        .calls[0] as unknown as [unknown, number, { signal: AbortSignal }];
      expect(remainingMs).toBeGreaterThan((timeout ?? 30000) - 100);
      expect(remainingMs).toBeLessThanOrEqual(timeout ?? 30000);
      expect(options.signal).toBeInstanceOf(AbortSignal);
      expect(next).toHaveBeenCalledOnce();
      expect(req.listenerCount('aborted')).toBe(2);
      res.writableEnded = true;
      res.emit('close');
      expect(req.listenerCount('aborted')).toBe(0);
      expect(res.listenerCount('close')).toBe(0);
    });
  }
});
