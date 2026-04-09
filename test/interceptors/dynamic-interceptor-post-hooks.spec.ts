import { ExecutionContext } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { lastValueFrom, of, throwError } from 'rxjs';
import { DynamicInterceptor } from '../../src/shared/interceptors/dynamic.interceptor';
import { ExecutorEngineService } from '../../src/infrastructure/executor-engine/services/executor-engine.service';

function makeReq(overrides: Record<string, any> = {}) {
  return {
    routeData: {
      preHooks: [],
      postHooks: [],
      context: {
        $body: {},
        $query: {},
        $params: {},
        $user: null,
        $share: { $logs: [] as any[] },
      },
      ...overrides,
    },
  } as any;
}

function makeExecCtx(
  req: any,
  res: any = { headersSent: false },
): ExecutionContext {
  return {
    switchToHttp: () => ({
      getRequest: () => req,
      getResponse: () => res,
    }),
  } as unknown as ExecutionContext;
}

describe('DynamicInterceptor', () => {
  let interceptor: DynamicInterceptor<unknown>;
  let registerMock: jest.Mock;

  beforeEach(async () => {
    registerMock = jest.fn();
    const moduleRef = await Test.createTestingModule({
      providers: [
        DynamicInterceptor,
        {
          provide: ExecutorEngineService,
          useValue: { register: registerMock },
        },
      ],
    }).compile();
    interceptor = moduleRef.get(DynamicInterceptor);
  });

  describe('pre-hook registration', () => {
    it('registers pre-hooks via handlerExecutorService.register', async () => {
      const req = makeReq({
        preHooks: [
          { code: '$ctx.$body.x = 1;' },
          { code: '$ctx.$body.y = 2;' },
        ],
      });
      const ctx = makeExecCtx(req);
      const obs = await interceptor.intercept(ctx, {
        handle: () => of({ ok: true }),
      });
      await lastValueFrom(obs);

      expect(registerMock).toHaveBeenCalledTimes(2);
      expect(registerMock).toHaveBeenCalledWith(req, {
        code: '$ctx.$body.x = 1;',
        type: 'preHook',
      });
      expect(registerMock).toHaveBeenCalledWith(req, {
        code: '$ctx.$body.y = 2;',
        type: 'preHook',
      });
    });

    it('skips pre-hooks without code', async () => {
      const req = makeReq({
        preHooks: [
          { code: '$ctx.$body.x = 1;' },
          { code: '' },
          { code: null },
          {},
        ],
      });
      const ctx = makeExecCtx(req);
      const obs = await interceptor.intercept(ctx, {
        handle: () => of({ ok: true }),
      });
      await lastValueFrom(obs);

      expect(registerMock).toHaveBeenCalledTimes(1);
    });

    it('does nothing when no pre-hooks', async () => {
      const req = makeReq({ preHooks: [] });
      const ctx = makeExecCtx(req);
      const obs = await interceptor.intercept(ctx, {
        handle: () => of({ ok: true }),
      });
      await lastValueFrom(obs);

      expect(registerMock).not.toHaveBeenCalled();
    });

    it('does nothing when preHooks is undefined', async () => {
      const req = makeReq();
      delete req.routeData.preHooks;
      const ctx = makeExecCtx(req);
      const obs = await interceptor.intercept(ctx, {
        handle: () => of({ ok: true }),
      });
      await lastValueFrom(obs);

      expect(registerMock).not.toHaveBeenCalled();
    });
  });

  describe('response log appending', () => {
    it('returns data as-is when no logs', async () => {
      const req = makeReq();
      const ctx = makeExecCtx(req);
      const obs = await interceptor.intercept(ctx, {
        handle: () => of({ items: [1, 2] }),
      });
      const out = await lastValueFrom(obs);

      expect(out).toEqual({ items: [1, 2] });
    });

    it('appends logs when $share.$logs is non-empty', async () => {
      const req = makeReq();
      req.routeData.context.$share.$logs = ['log1', 'log2'];
      const ctx = makeExecCtx(req);
      const obs = await interceptor.intercept(ctx, {
        handle: () => of({ items: [1] }),
      });
      const out = await lastValueFrom(obs);

      expect(out).toEqual({ items: [1], logs: ['log1', 'log2'] });
    });

    it('returns undefined when headers already sent', async () => {
      const req = makeReq();
      const res = { headersSent: true };
      const ctx = makeExecCtx(req, res);
      const obs = await interceptor.intercept(ctx, {
        handle: () => of({ items: [1] }),
      });
      const out = await lastValueFrom(obs);

      expect(out).toBeUndefined();
    });
  });

  describe('error handling', () => {
    it('attaches logs to error when $share.$logs is non-empty', async () => {
      const req = makeReq();
      req.routeData.context.$share.$logs = ['err-log'];
      const ctx = makeExecCtx(req);
      const testError = new Error('handler failed');
      const obs = await interceptor.intercept(ctx, {
        handle: () => throwError(() => testError),
      });

      await expect(lastValueFrom(obs)).rejects.toThrow('handler failed');
      expect((testError as any).logs).toEqual(['err-log']);
    });

    it('does not attach logs to error when $share.$logs is empty', async () => {
      const req = makeReq();
      const ctx = makeExecCtx(req);
      const testError = new Error('handler failed');
      const obs = await interceptor.intercept(ctx, {
        handle: () => throwError(() => testError),
      });

      await expect(lastValueFrom(obs)).rejects.toThrow('handler failed');
      expect((testError as any).logs).toBeUndefined();
    });

    it('handles missing routeData gracefully in catchError', async () => {
      const req = { routeData: undefined } as any;
      const ctx = makeExecCtx(req);
      const testError = new Error('no route');

      const obs = await interceptor.intercept(ctx, {
        handle: () => throwError(() => testError),
      });

      await expect(lastValueFrom(obs)).rejects.toThrow('no route');
      expect((testError as any).logs).toBeUndefined();
    });
  });
});
