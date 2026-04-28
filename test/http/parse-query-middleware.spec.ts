import { describe, expect, it, vi } from 'vitest';
import { parseQueryMiddleware } from '../../src/http/middlewares';

describe('parseQueryMiddleware aggregate parsing', () => {
  it('parses aggregate JSON query params', () => {
    const req: any = {
      query: {
        aggregate: JSON.stringify({
          amount: { sum: { _gt: 0 }, count: true },
        }),
        filter: JSON.stringify({ status: { _eq: 'paid' } }),
        deep: JSON.stringify({ owner: { fields: 'id,name' } }),
        search: 'plain',
      },
    };
    const next = vi.fn();

    parseQueryMiddleware(req, {} as any, next);

    expect(req.query.aggregate).toEqual({
      amount: { sum: { _gt: 0 }, count: true },
    });
    expect(req.query.filter).toEqual({ status: { _eq: 'paid' } });
    expect(req.query.deep).toEqual({ owner: { fields: 'id,name' } });
    expect(req.query.search).toBe('plain');
    expect(next).toHaveBeenCalledTimes(1);
  });

  it('leaves invalid aggregate JSON as the original string', () => {
    const req: any = {
      query: {
        aggregate: '{"amount":',
      },
    };
    const next = vi.fn();

    parseQueryMiddleware(req, {} as any, next);

    expect(req.query.aggregate).toBe('{"amount":');
    expect(next).toHaveBeenCalledTimes(1);
  });
});
