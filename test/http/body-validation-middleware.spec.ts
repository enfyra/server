import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  bodyValidationMiddleware,
  invalidateBodyValidationCache,
} from '../../src/http/middlewares/body-validation.middleware';

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
    getMetadata: async () => metadata,
  };
  const ruleCache: any = {
    getCacheAsync: async () => opts.rulesByColumn ?? new Map(),
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

  it('GET request → passes through', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'GET',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: {},
    });
    await mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });

  it('DELETE → passes through', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'DELETE',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: {},
    });
    await mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });

  it('No mainTable → passes through', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: {},
      body: {},
    });
    await mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });

  it('mainTable.validateBody=false → passes through (opt-out)', async () => {
    const off = { ...tableMeta, validateBody: false };
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta: off }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: off, path: '/' + off.name },
      body: {},
    });
    await mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });

  it('mainTable.validateBody=undefined → validates by default (auto-on)', async () => {
    const auto = { ...tableMeta };
    delete auto.validateBody;
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta: auto }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: auto, path: '/' + auto.name },
      body: {},
    });
    await mw(req, res, next);
    const err = next.mock.calls[0]?.[0];
    if (!err || !/title is required/.test((err.messages || []).join('|'))) {
      throw new Error(
        `Expected "title is required", got: ${JSON.stringify(err?.messages || err?.message)}`,
      );
    }
  });

  it('Non-canonical path (custom route) → passes through', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/auth/login' },
      body: {},
    });
    await mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });

  it('Canonical collection path /:tableName → validates', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/post' },
      body: {},
    });
    await mw(req, res, next);
    const err = next.mock.calls[0]?.[0];
    if (!err || !/title is required/.test((err.messages || []).join('|'))) {
      throw new Error(
        `Expected "title is required", got: ${JSON.stringify(err?.messages || err?.message)}`,
      );
    }
  });

  it('Canonical item path /:tableName/:id → validates (PATCH)', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'PATCH',
      routeData: { mainTable: tableMeta, path: '/post/:id' },
      body: { title: 123 },
    });
    await mw(req, res, next);
    const err = next.mock.calls[0]?.[0];
    if (
      !err ||
      !/title must be a string/.test((err.messages || []).join('|'))
    ) {
      throw new Error(
        `Expected "title must be a string", got: ${JSON.stringify(err?.messages || err?.message)}`,
      );
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

  it('valid body passes', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: { title: 'Hello' },
    });
    await mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });

  it('missing required field throws BadRequest', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: {},
    });
    await mw(req, res, next);
    const err = next.mock.calls[0]?.[0];
    if (!err || !/title is required/.test((err.messages || []).join('|'))) {
      throw new Error(
        `Expected "title is required", got: ${JSON.stringify(err?.messages || err?.message)}`,
      );
    }
  });

  it('wrong type throws BadRequest', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: { title: 123 },
    });
    await mw(req, res, next);
    const err = next.mock.calls[0]?.[0];
    if (
      !err ||
      !/title must be a string/.test((err.messages || []).join('|'))
    ) {
      throw new Error(
        `Expected "title must be a string", got: ${JSON.stringify(err?.messages || err?.message)}`,
      );
    }
  });

  it('unknown key rejected (strict)', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: { title: 'x', malicious: 1 },
    });
    await mw(req, res, next);
    const err = next.mock.calls[0]?.[0];
    if (
      !err ||
      !/malicious is not allowed/.test((err.messages || []).join('|'))
    ) {
      throw new Error(
        `Expected "malicious is not allowed", got: ${JSON.stringify(err?.messages || err?.message)}`,
      );
    }
  });

  it('non-null generated columns are not required by request validation', async () => {
    const generatedMeta = {
      ...tableMeta,
      columns: [
        ...tableMeta.columns,
        {
          id: 'c3',
          name: 'serverValue',
          type: 'varchar',
          isGenerated: true,
          isNullable: false,
        },
      ],
    };
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta: generatedMeta }));
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: generatedMeta, path: '/' + generatedMeta.name },
      body: { title: 'Hello' },
    });
    await mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });
});

describe('bodyValidationMiddleware — PATCH update mode', () => {
  const tableMeta = {
    name: 'post',
    validateBody: true,
    columns: [{ id: 'c1', name: 'title', type: 'varchar', isNullable: false }],
    relations: [],
  };

  it('PATCH with partial body passes (all fields optional)', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'PATCH',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: {},
    });
    await mw(req, res, next);
    expect(next).toHaveBeenCalledWith();
  });

  it('PATCH with wrong type still rejected', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { req, res, next } = makeReqRes({
      method: 'PATCH',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: { title: 123 },
    });
    await mw(req, res, next);
    const err = next.mock.calls[0]?.[0];
    if (
      !err ||
      !/title must be a string/.test((err.messages || []).join('|'))
    ) {
      throw new Error(
        `Expected "title must be a string", got: ${JSON.stringify(err?.messages || err?.message)}`,
      );
    }
  });
});

describe('bodyValidationMiddleware — column rules applied', () => {
  const tableMeta = {
    name: 'user',
    validateBody: true,
    columns: [{ id: 'c1', name: 'email', type: 'varchar', isNullable: false }],
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

  it('format:email rule enforced', async () => {
    const mw = bodyValidationMiddleware(
      makeContainer({ tableMeta, rulesByColumn: rules }),
    );
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: { email: 'nope' },
    });
    await mw(req, res, next);
    const err = next.mock.calls[0]?.[0];
    if (!err || !/email/.test((err.messages || []).join('|'))) {
      throw new Error(
        `Expected email error, got: ${JSON.stringify(err?.messages || err?.message)}`,
      );
    }
  });

  it('valid email passes', async () => {
    const mw = bodyValidationMiddleware(
      makeContainer({ tableMeta, rulesByColumn: rules }),
    );
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
      body: { email: 'a@b.com' },
    });
    await mw(req, res, next);
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

  it('second request with same metadata version reuses cached schema', async () => {
    const container = makeContainer({ tableMeta });
    const getMetaSpy = vi.spyOn(
      container.cradle.metadataCacheService,
      'getMetadata',
    );
    getMetaSpy.mockResolvedValue({
      tables: new Map([[tableMeta.name, tableMeta]]),
      tablesList: [tableMeta],
      version: 1,
      timestamp: new Date(),
    } as any);
    const mw = bodyValidationMiddleware(container);
    await mw(
      {
        method: 'POST',
        routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
        body: { title: 'x' },
      } as any,
      makeReqRes({}).res,
      makeReqRes({}).next,
    );
    const firstTotal = getMetaSpy.mock.calls.length;
    await mw(
      {
        method: 'POST',
        routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
        body: { title: 'y' },
      } as any,
      makeReqRes({}).res,
      makeReqRes({}).next,
    );
    const secondTotal = getMetaSpy.mock.calls.length;

    // Schema built once (2 getMetadata calls: version + build).
    // Second request: only version lookup (1 call), no build.
    expect(secondTotal - firstTotal).toBeLessThanOrEqual(1);
  });

  it('metadata version change triggers rebuild', async () => {
    const container = makeContainer({ tableMeta });
    let currentVersion = 1;
    const getMetaSpy = vi.spyOn(
      container.cradle.metadataCacheService,
      'getMetadata',
    );
    getMetaSpy.mockImplementation(
      async () =>
        ({
          tables: new Map([[tableMeta.name, tableMeta]]),
          tablesList: [tableMeta],
          version: currentVersion,
          timestamp: new Date(),
        }) as any,
    );
    const mw = bodyValidationMiddleware(container);
    await mw(
      {
        method: 'POST',
        routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
        body: { title: 'x' },
      } as any,
      makeReqRes({}).res,
      makeReqRes({}).next,
    );
    const firstTotal = getMetaSpy.mock.calls.length;

    currentVersion = 2; // bump — new cache key
    await mw(
      {
        method: 'POST',
        routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
        body: { title: 'y' },
      } as any,
      makeReqRes({}).res,
      makeReqRes({}).next,
    );
    const secondTotal = getMetaSpy.mock.calls.length;

    // Rebuild needed → expect 2+ additional calls (version + schema build)
    expect(secondTotal - firstTotal).toBeGreaterThanOrEqual(2);
  });

  it('invalidateBodyValidationCache() rebuilds on next call', async () => {
    const container = makeContainer({ tableMeta });
    const getMetaSpy = vi.spyOn(
      container.cradle.metadataCacheService,
      'getMetadata',
    );
    const mw = bodyValidationMiddleware(container);
    await mw(
      {
        method: 'POST',
        routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
        body: { title: 'x' },
      } as any,
      makeReqRes({}).res,
      makeReqRes({}).next,
    );
    const beforeInv = getMetaSpy.mock.calls.length;

    invalidateBodyValidationCache();
    await mw(
      {
        method: 'POST',
        routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
        body: { title: 'y' },
      } as any,
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
      throw new Error(
        `Error mismatch: ${JSON.stringify(err.messages || err.message)}`,
      );
    }
  }

  it('body = null → next(BadRequest) with "body is required"', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { res, next } = makeReqRes({});
    await mw(
      {
        method: 'POST',
        routeData: { mainTable: tableMeta, path: '/post' },
        body: null,
      } as any,
      res,
      next,
    );
    expectNextError(next, /body is required/);
  });

  it('body = undefined → next(BadRequest)', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { res, next } = makeReqRes({});
    await mw(
      {
        method: 'POST',
        routeData: { mainTable: tableMeta, path: '/post' },
        body: undefined,
      } as any,
      res,
      next,
    );
    expectNextError(next, /body is required/);
  });

  it('body = array → next(BadRequest)', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { res, next } = makeReqRes({});
    await mw(
      {
        method: 'POST',
        routeData: { mainTable: tableMeta, path: '/post' },
        body: [{ title: 'x' }],
      } as any,
      res,
      next,
    );
    expectNextError(next, /body must be an object, not an array/);
  });

  it('body = string → next(BadRequest)', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { res, next } = makeReqRes({});
    await mw(
      {
        method: 'POST',
        routeData: { mainTable: tableMeta, path: '/post' },
        body: 'raw',
      } as any,
      res,
      next,
    );
    expectNextError(next, /body must be an object/);
  });

  it('body = number → next(BadRequest)', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { res, next } = makeReqRes({});
    await mw(
      {
        method: 'POST',
        routeData: { mainTable: tableMeta, path: '/post' },
        body: 42,
      } as any,
      res,
      next,
    );
    expectNextError(next, /body must be an object/);
  });
});

describe('bodyValidationMiddleware — metadata cache null (cold start)', () => {
  it('getMetadata returns null → middleware passes through (no validation)', async () => {
    const tableMeta = {
      name: 'post',
      validateBody: true,
      columns: [
        { id: 'c1', name: 'title', type: 'varchar', isNullable: false },
      ],
      relations: [],
    };
    const container = makeContainer({ tableMeta });
    (container.cradle.metadataCacheService as any).getMetadata = async () =>
      null;
    const mw = bodyValidationMiddleware(container);
    const { req, res, next } = makeReqRes({
      method: 'POST',
      routeData: { mainTable: tableMeta, path: '/post' },
      body: {},
    });
    await mw(req, res, next);
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

  it('error passed to next() is BadRequestException with messages string[]', async () => {
    const mw = bodyValidationMiddleware(makeContainer({ tableMeta }));
    const { res, next } = makeReqRes({});
    await mw(
      {
        method: 'POST',
        routeData: { mainTable: tableMeta, path: '/' + tableMeta.name },
        body: {},
      } as any,
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
