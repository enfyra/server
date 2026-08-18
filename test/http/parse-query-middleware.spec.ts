import { describe, expect, it, vi } from 'vitest';
import { parseQueryMiddleware } from '../../src/http/middlewares/parse-query.middleware';

describe('parseQueryMiddleware', () => {
  it('parses filter and deep JSON query params but leaves aggregate unparsed', () => {
    const req: any = {
      query: {
        aggregate: JSON.stringify({ dimensions: [], measures: {} }),
        filter: JSON.stringify({ status: { _eq: 'paid' } }),
        deep: JSON.stringify({ owner: { fields: 'id,name' } }),
        search: 'plain',
      },
    };
    const next = vi.fn();

    parseQueryMiddleware(req, {} as any, next);

    expect(req.query.aggregate).toBe(JSON.stringify({ dimensions: [], measures: {} }));
    expect(req.query.filter).toEqual({ status: { _eq: 'paid' } });
    expect(req.query.deep).toEqual({ owner: { fields: 'id,name' } });
    expect(req.query.search).toBe('plain');
    expect(next).toHaveBeenCalledTimes(1);
  });
});
