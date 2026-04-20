import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  bodyValidationMiddleware,
  invalidateBodyValidationCache,
} from '../../src/http/middleware/body-validation.middleware';

function makeMetadata(tableMeta: any) {
  const tables = new Map([[tableMeta.name, tableMeta]]);
  return { tables, tablesList: [tableMeta], version: 1, timestamp: new Date() };
}

function makeContainer(opts: {
  tableMeta: any;
  rulesByColumn?: Map<string, any[]>;
}) {
  const metadata = makeMetadata(opts.tableMeta);
  const metadataCache: any = {
    getDirectMetadata: () => metadata,
  };
  const ruleCache: any = {
    getRulesForColumnSync: (id: string | number) =>
      opts.rulesByColumn?.get(String(id)) ?? [],
  };
  const eventEmitter = { on: vi.fn() };
  return {
    cradle: {
      metadataCacheService: metadataCache,
      columnRuleCacheService: ruleCache,
      eventEmitter,
    },
  } as any;
}

function makeReqRes(req: any) {
  const res: any = {
    status: vi.fn().mockReturnThis(),
    json: vi.fn().mockReturnThis(),
  };
  const next = vi.fn();
  return { req, res, next };
}

beforeEach(() => {
  invalidateBodyValidationCache();
});

describe('bodyValidationMiddleware — skip conditions', () => {
  const tableMeta = {
    name: 'post',
    validateBody: true,
    columns: [{ id: 'c1', name: 'title', type: 'varchar', isNullable: false }],
    relations: [],
  };

  it('GET request → passes through', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'GET',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: {},
    });
    mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });

  it('DELETE → passes through', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'DELETE',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: {},
    });
    mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });

  it('No mainTable → passes through', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: {},
      body: {},
    });
    mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });

  it('mainTable.validateBody=false → passes through (opt-out)', () => {
    const off = { ...tableMeta, validateBody: false };
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta: off }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: off, path: '/' + off.name },
      body: {},
    });
    mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });

  it('mainTable.validateBody=undefined → validates by default (auto-on)', () => {
    const auto = { ...tableMeta };
    delete auto.validateBody;
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta: auto }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: auto, path: '/' + auto.name },
      body: {},
    });
    mw(req, res, next);
    const err = next.mock.calls[0]?.[0];
    if (!err || !/title is required/.test((err.messages || []).join('|'))) {
      throw new Error(`Expected "title is required", got: ${JSON.stringify(err?.messages || err?.message)}`);
    }
  });

  it('Non-canonical path (custom route) → passes through', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/auth/login' },
      body: {},
    });
    mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });

  it('Canonical collection path /:tableName → validates', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/post' },
      body: {},
    });
    mw(req, res, next);
    const err = next.mock.calls[0]?.[0];
    if (!err || !/title is required/.test((err.messages || []).join('|'))) {
      throw new Error(`Expected "title is required", got: ${JSON.stringify(err?.messages || err?.message)}`);
    }
  });

  it('Canonical item path /:tableName/:id → validates (PATCH)', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'PATCH',
      routeData: { mainTable: tableMeta, path: '/post/:id' },
      body: { title: 123 },
    });
    mw(req, res, next);
    const err = next.mock.calls[0]?.[0];
    if (!err || !/title must be a string/.test((err.messages || []).join('|'))) {
      throw new Error(`Expected "title must be a string", got: ${JSON.stringify(err?.messages || err?.message)}`);
    }
  });
});

describe('bodyValidationMiddleware — POST validates on create', () => {
  const tableMeta = {
    name: 'post',
    validateBody: true,
    columns: [
      { id: 'c1', name: 'title', type: 'varchar', isNullable: false },
      { id: 'c2', name: 'body', type: 'text', isNullable: true },
    ],
    relations: [],
  };

  it('valid body passes', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: { title: 'Hello' },
    });
    mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });

  it('missing required field throws BadRequest', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: {},
    });
    mw(req, res, next);
    const err = next.mock.calls[0]?.[0];
    if (!err || !/title is required/.test((err.messages || []).join('|'))) {
      throw new Error(`Expected "title is required", got: ${JSON.stringify(err?.messages || err?.message)}`);
    }
  });

  it('wrong type throws BadRequest', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: { title: 123 },
    });
    mw(req, res, next);
    const err = next.mock.calls[0]?.[0];
    if (!err || !/title must be a string/.test((err.messages || []).join('|'))) {
      throw new Error(`Expected "title must be a string", got: ${JSON.stringify(err?.messages || err?.message)}`);
    }
  });

  it('unknown key rejected (strict)', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: { title: 'x', malicious: 1 },
    });
    mw(req, res, next);
    const err = next.mock.calls[0]?.[0];
    if (!err || !/malicious is not allowed/.test((err.messages || []).join('|'))) {
      throw new Error(`Expected "malicious is not allowed", got: ${JSON.stringify(err?.messages || err?.message)}`);
    }
  });
});

describe('bodyValidationMiddleware — PATCH update mode', () => {
  const tableMeta = {
    name: 'post',
    validateBody: true,
    columns: [
      { id: 'c1', name: 'title', type: 'varchar', isNullable: false },
    ],
    relations: [],
  };

  it('PATCH with partial body passes (all fields optional)', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'PATCH',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: {},
    });
    mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });

  it('PATCH with wrong type still rejected', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'PATCH',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: { title: 123 },
    });
    mw(req, res, next);
    const err = next.mock.calls[0]?.[0];
    if (!err || !/title must be a string/.test((err.messages || []).join('|'))) {
      throw new Error(`Expected "title must be a string", got: ${JSON.stringify(err?.messages || err?.message)}`);
    }
  });
});

describe('bodyValidationMiddleware — column rules applied', () => {
  const tableMeta = {
    name: 'user',
    validateBody: true,
    columns: [
      { id: 'c1', name: 'email', type: 'varchar', isNullable: false },
    ],
    relations: [],
  };
  const rules = new Map<string, any[]>();
  rules.set('c1', [
    {
      id: 'r1',
      ruleType: 'format',
      value: { v: 'email' },
      message: null,
      isEnabled: true,
      columnId: 'c1',
    },
  ]);

  it('format:email rule enforced', () => {
    const mw = bodyValidationMiddleware(
      makeContainer({ tableMeta, rulesByColumn: rules }),
    );
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: { email: 'nope' },
    });
    mw(req, res, next);
    const err = next.mock.calls[0]?.[0];
    if (!err || !/email/.test((err.messages || []).join('|'))) {
      throw new Error(`Expected email error, got: ${JSON.stringify(err?.messages || err?.message)}`);
    }
  });

  it('valid email passes', () => {
    const mw = bodyValidationMiddleware(
      makeContainer({ tableMeta, rulesByColumn: rules }),
    );
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: { email: 'a@b.com' },
    });
    mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });
});

describe('bodyValidationMiddleware — schema caching', () => {
  const tableMeta = {
    name: 'post',
    validateBody: true,
    columns: [{ id: 'c1', name: 'title', type: 'varchar', isNullable: false }],
    relations: [],
  };

  it('second request with same metadata version reuses cached schema', () => {
    const container = makeContainer({ tableMeta });
    const getMetaSpy = vi.spyOn(
      container.cradle.metadataCacheService,
      'getDirectMetadata',
    );
    getMetaSpy.mockReturnValue({
      tables: new Map([[tableMeta.name, tableMeta]]),
      tablesList: [tableMeta],
      version: 1,
      timestamp: new Date(),
    } as any);
    const mw = bodyValidationMiddleware(container);

    mw(
      { method: 'POST', routeData: { mainTable: tableMeta, path: '/' + tableMeta.name }, body: { title: 'x' } } as any,
      makeReqRes({}).res,
      makeReqRes({}).next,
    );
    const firstTotal = getMetaSpy.mock.calls.length;

    mw(
      { method: 'POST', routeData: { mainTable: tableMeta, path: '/' + tableMeta.name }, body: { title: 'y' } } as any,
      makeReqRes({}).res,
      makeReqRes({}).next,
    );
    const secondTotal = getMetaSpy.mock.calls.length;

    // Schema built once (2 getDirectMetadata calls: version + build).
    // Second request: only version lookup (1 call), no build.
    expect(secondTotal - firstTotal).toBeLessThanOrEqual(1);
  });

  it('metadata version change triggers rebuild', () => {
    const container = makeContainer({ tableMeta });
    let currentVersion = 1;
    const getMetaSpy = vi.spyOn(
      container.cradle.metadataCacheService,
      'getDirectMetadata',
    );
    getMetaSpy.mockImplementation(
      () =>
        ({
          tables: new Map([[tableMeta.name, tableMeta]]),
          tablesList: [tableMeta],
          version: currentVersion,
          timestamp: new Date(),
        }) as any,
    );
    const mw = bodyValidationMiddleware(container);

    mw(
      { method: 'POST', routeData: { mainTable: tableMeta, path: '/' + tableMeta.name }, body: { title: 'x' } } as any,
      makeReqRes({}).res,
      makeReqRes({}).next,
    );
    const firstTotal = getMetaSpy.mock.calls.length;

    currentVersion = 2; // bump — new cache key

    mw(
      { method: 'POST', routeData: { mainTable: tableMeta, path: '/' + tableMeta.name }, body: { title: 'y' } } as any,
      makeReqRes({}).res,
      makeReqRes({}).next,
    );
    const secondTotal = getMetaSpy.mock.calls.length;

    // Rebuild needed → expect 2+ additional calls (version + schema build)
    expect(secondTotal - firstTotal).toBeGreaterThanOrEqual(2);
  });

  it('invalidateBodyValidationCache() rebuilds on next call', () => {
    const container = makeContainer({ tableMeta });
    const getMetaSpy = vi.spyOn(
      container.cradle.metadataCacheService,
      'getDirectMetadata',
    );
    const mw = bodyValidationMiddleware(container);

    mw(
      { method: 'POST', routeData: { mainTable: tableMeta, path: '/' + tableMeta.name }, body: { title: 'x' } } as any,
      makeReqRes({}).res,
      makeReqRes({}).next,
    );
    const beforeInv = getMetaSpy.mock.calls.length;

    invalidateBodyValidationCache();

    mw(
      { method: 'POST', routeData: { mainTable: tableMeta, path: '/' + tableMeta.name }, body: { title: 'y' } } as any,
      makeReqRes({}).res,
      makeReqRes({}).next,
    );
    expect(getMetaSpy.mock.calls.length).toBeGreaterThan(beforeInv);
  });
});

describe('bodyValidationMiddleware — defensive body handling', () => {
  const tableMeta = {
    name: 'post',
    validateBody: true,
    columns: [{ id: 'c1', name: 'title', type: 'varchar', isNullable: false }],
    relations: [],
  };

  function expectNextError(next: any, regex: RegExp) {
    const err = next.mock.calls[0]?.[0];
    if (!err) throw new Error('next not called with error');
    if (!regex.test((err.messages || []).join('|') || err.message || '')) {
      throw new Error(`Error mismatch: ${JSON.stringify(err.messages || err.message)}`);
    }
  }

  it('body = null → next(BadRequest) with "body is required"', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { res, next } = makeReqRes({});
    mw(
      { method: 'POST', routeData: { mainTable: tableMeta, path: '/post' }, body: null } as any,
      res,
      next,
    );
    expectNextError(next, /body is required/);
  });

  it('body = undefined → next(BadRequest)', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { res, next } = makeReqRes({});
    mw(
      { method: 'POST', routeData: { mainTable: tableMeta, path: '/post' }, body: undefined } as any,
      res,
      next,
    );
    expectNextError(next, /body is required/);
  });

  it('body = array → next(BadRequest)', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { res, next } = makeReqRes({});
    mw(
      { method: 'POST', routeData: { mainTable: tableMeta, path: '/post' }, body: [{ title: 'x' }] } as any,
      res,
      next,
    );
    expectNextError(next, /body must be an object, not an array/);
  });

  it('body = string → next(BadRequest)', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { res, next } = makeReqRes({});
    mw(
      { method: 'POST', routeData: { mainTable: tableMeta, path: '/post' }, body: 'raw' } as any,
      res,
      next,
    );
    expectNextError(next, /body must be an object/);
  });

  it('body = number → next(BadRequest)', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { res, next } = makeReqRes({});
    mw(
      { method: 'POST', routeData: { mainTable: tableMeta, path: '/post' }, body: 42 } as any,
      res,
      next,
    );
    expectNextError(next, /body must be an object/);
  });
});

describe('bodyValidationMiddleware — metadata cache null (cold start)', () => {
  it('getDirectMetadata returns null → middleware passes through (no validation)', () => {
    const tableMeta = {
      name: 'post',
      validateBody: true,
      columns: [{ id: 'c1', name: 'title', type: 'varchar', isNullable: false }],
      relations: [],
    };
    const container = makeContainer({ tableMeta });
    (container.cradle.metadataCacheService as any).getDirectMetadata = () => null;
    const mw = bodyValidationMiddleware(container);
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/post' },
      body: {},
    });
    mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });
});

describe('bodyValidationMiddleware — error format', () => {
  const tableMeta = {
    name: 'user',
    validateBody: true,
    columns: [
      { id: 'c1', name: 'email', type: 'varchar', isNullable: false },
      { id: 'c2', name: 'age', type: 'int', isNullable: false },
    ],
    relations: [],
  };

  it('error passed to next() is BadRequestException with messages string[]', () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { res, next } = makeReqRes({});
    mw(
      { method: 'POST', routeData: { mainTable: tableMeta, path: '/' + tableMeta.name }, body: {} } as any,
      res,
      next,
    );
    const err = next.mock.calls[0]?.[0];
    expect(err).toBeDefined();
    expect(err.statusCode).toBe(400);
    expect(Array.isArray(err.messages)).toBe(true);
    expect(err.messages).toContain('email is required');
    expect(err.messages).toContain('age is required');
  });
});
