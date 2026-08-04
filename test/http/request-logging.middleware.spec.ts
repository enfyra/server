import { afterEach, describe, expect, it, vi } from 'vitest';
import { Logger } from '../../src/shared/logger';
import { requestLoggingEnd } from '../../src/http/middlewares/request-logging.middleware';

describe('request logging middleware', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('writes client error responses at warning level', () => {
    const warn = vi.spyOn(Logger.prototype, 'warn');
    const next = vi.fn();

    requestLoggingEnd(
      {
        method: 'POST',
        url: '/gateway/v1/messages',
        query: {},
        startTime: Date.now(),
        user: { id: 7 },
      } as any,
      { statusCode: 400 } as any,
      next,
    );

    expect(warn).toHaveBeenCalledWith(
      expect.objectContaining({
        method: 'POST',
        url: '/gateway/v1/messages',
        statusCode: 400,
        userId: 7,
      }),
    );
    expect(next).toHaveBeenCalledOnce();
  });
});
