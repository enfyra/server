const SYSTEM_EVENT_PREFIX = '$system:package';

function sleep(ms: number) {
  return new Promise<void>((r) => setTimeout(r, ms));
}

function createMockQueryBuilder(
  records: Map<string | number, any> = new Map(),
) {
  let nextId = 100;
  return {
    records,
    insert: jest.fn(async (_table: string, data: any) => {
      const id = nextId++;
      const record = { id, ...data };
      records.set(id, record);
      return record;
    }),
    insertAndGet: jest.fn(async (_table: string, data: any) => {
      const id = nextId++;
      const record = { id, ...data };
      records.set(id, record);
      return record;
    }),
    update: jest.fn(
      async (...args: any[]) => {
        if (args.length === 3 && typeof args[1] === 'object' && !Array.isArray(args[1])) {
          const [_table, whereOpts, data] = args;
          const idCondition = whereOpts.where?.find((w: any) => w.field === 'id');
          if (idCondition) {
            const key = idCondition.value;
            const existing =
              records.get(key) ||
              records.get(Number(key)) ||
              records.get(String(key));
            if (existing) {
              Object.assign(existing, data);
            }
          }
          return [{ id: idCondition?.value, ...data }];
        }
        const [_table, id, data] = args;
        const existing =
          records.get(id) || records.get(Number(id));
        if (existing) {
          Object.assign(existing, data);
        }
        return { id, ...data };
      },
    ),
    find: jest.fn(async (opts: any) => {
      const all = Array.from(records.values());
      let filtered = all;
      if (opts.filter) {
        filtered = all.filter((r: any) => {
          for (const [k, v] of Object.entries(opts.filter)) {
            if (r[k] !== v) return false;
          }
          return true;
        });
      }
      if (opts.fields) {
        filtered = filtered.map((r: any) => {
          const out: any = {};
          for (const f of opts.fields) out[f] = r[f];
          return out;
        });
      }
      return { data: filtered };
    }),
    select: jest.fn(async (opts: any) => {
      const all = Array.from(records.values());
      let filtered = all;
      if (opts.filter) {
        filtered = all.filter((r: any) => {
          for (const [k, v] of Object.entries(opts.filter)) {
            if (r[k] !== v) return false;
          }
          return true;
        });
      }
      if (opts.fields) {
        filtered = filtered.map((r: any) => {
          const out: any = {};
          for (const f of opts.fields) out[f] = r[f];
          return out;
        });
      }
      return { data: filtered };
    }),
    delete: jest.fn(async (...args: any[]) => {
      if (args.length === 2 && typeof args[1] === 'object') {
        const [_table, opts] = args;
        const idCondition = opts.where?.find((w: any) => w.field === 'id');
        if (idCondition) {
          records.delete(Number(idCondition.value));
          records.delete(idCondition.value);
        }
      } else {
        const [_table, id] = args;
        records.delete(Number(id));
        records.delete(id);
      }
    }),
    isMongoDb: jest.fn(() => false),
    getPkField: jest.fn(() => 'id'),
  };
}

function createMockPackageRepo(
  queryBuilder: ReturnType<typeof createMockQueryBuilder>,
) {
  return {
    find: jest.fn(async (opts: any) => {
      const where = opts.where || {};
      const all = Array.from(queryBuilder.records.values());
      const filtered = all.filter((r: any) => {
        for (const [k, condition] of Object.entries(where)) {
          if (typeof condition === 'object' && condition !== null) {
            const cond = condition as any;
            if (cond._eq !== undefined) {
              if (String(r[k]) !== String(cond._eq)) return false;
            }
          }
        }
        return true;
      });
      return { data: filtered };
    }),
    update: jest.fn(async (opts: any) => {
      const numId = Number(opts.id);
      const record =
        queryBuilder.records.get(numId) || queryBuilder.records.get(opts.id);
      if (record) Object.assign(record, opts.data);
      return { data: [record] };
    }),
    delete: jest.fn(async (opts: any) => {
      const numId = Number(opts.id);
      queryBuilder.records.delete(numId);
      queryBuilder.records.delete(opts.id);
      return { data: [] };
    }),
  };
}

function createMockWebSocketGateway() {
  const events: Array<{ event: string; data: any }> = [];
  return {
    events,
    emitToNamespace: jest.fn((_path: string, event: string, data: any) => {
      events.push({ event, data });
    }),
    getEventsFor(suffix: string) {
      return events.filter(
        (e) => e.event === `${SYSTEM_EVENT_PREFIX}:${suffix}`,
      );
    },
    clearEvents() {
      events.length = 0;
    },
  };
}

function createMockCdnLoader(
  overrides: Partial<{
    loadPackage: jest.Mock;
    invalidatePackage: jest.Mock;
    isLoaded: jest.Mock;
  }> = {},
) {
  const loaded = new Set<string>();
  const loadPackage =
    overrides.loadPackage ??
    jest.fn(async (name: string) => {
      loaded.add(name);
      return {};
    });
  const isLoadedFn =
    overrides.isLoaded ?? jest.fn((name: string) => loaded.has(name));
  return {
    loaded,
    loadPackage,
    invalidatePackage: overrides.invalidatePackage ?? jest.fn(async () => {}),
    isLoaded: isLoadedFn,
    getLoadedPackages: jest.fn(() => new Map()),
    getPackageSources: jest.fn(() => []),
    preloadPackages: jest.fn(),
    invalidateAll: jest.fn(),
  };
}

function createMockPackageCacheService() {
  return {
    reload: jest.fn(async () => {}),
  };
}

async function buildController(opts: {
  packageCacheService: ReturnType<typeof createMockPackageCacheService>;
  cdnLoader: ReturnType<typeof createMockCdnLoader>;
  queryBuilder: ReturnType<typeof createMockQueryBuilder>;
  websocketGateway: ReturnType<typeof createMockWebSocketGateway>;
}) {
  const mod =
    await import('../../src/modules/package-management/controllers/package.controller');
  const eventEmitter = { emit: jest.fn() };
  const ctrl = new (mod.PackageController as any)(
    opts.cdnLoader,
    opts.queryBuilder,
    opts.websocketGateway,
    eventEmitter,
  );
  return { ctrl, eventEmitter };
}

function makeReq(body: any, repo: any) {
  return {
    routeData: {
      context: {
        $body: body,
        $repos: { main: repo },
      },
    },
  } as any;
}

async function buildCacheService(opts: {
  dbPackages: Array<{
    id: number;
    name: string;
    version: string;
    type: string;
    isEnabled: boolean;
    status: string;
  }>;
  cdnLoader: ReturnType<typeof createMockCdnLoader>;
  ws: ReturnType<typeof createMockWebSocketGateway>;
}) {
  const qb = createMockQueryBuilder();
  for (const pkg of opts.dbPackages) {
    qb.records.set(pkg.id, { ...pkg });
  }

  const eventEmitter = {
    emit: jest.fn(),
    on: jest.fn(),
    onAny: jest.fn(),
  };

  const mod =
    await import('../../src/infrastructure/cache/services/package-cache.service');
  const service = new (mod.PackageCacheService as any)(
    qb,
    eventEmitter,
    opts.ws,
    opts.cdnLoader,
  );

  service.reload = jest.fn(async () => {});

  return { service, qb, eventEmitter };
}

describe('PackageController – install', () => {
  it('returns quickly with status=installing for Server packages', async () => {
    const qb = createMockQueryBuilder();
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader({
      loadPackage: jest.fn(async () => {
        await sleep(200);
        return {};
      }),
    });
    const cache = createMockPackageCacheService();
    const { ctrl, eventEmitter } = await buildController({
      packageCacheService: cache,
      cdnLoader: cdn,
      queryBuilder: qb,
      websocketGateway: ws,
    });

    const repo = createMockPackageRepo(qb);
    const req = makeReq(
      { name: 'test-pkg', type: 'Server', version: '3.0.0' },
      repo,
    );

    const t0 = Date.now();
    const result = await ctrl.installPackage(req);
    const elapsed = Date.now() - t0;

    expect(elapsed).toBeLessThan(100);
    expect(result.data[0].status).toBe('installing');
    expect(result.data[0].name).toBe('test-pkg');

    const installingEvents = ws.getEventsFor('installing');
    expect(installingEvents.length).toBe(1);
    expect(installingEvents[0].data.name).toBe('test-pkg');
  });

  it('sets failed after CDN load throws', async () => {
    const qb = createMockQueryBuilder();
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader({
      loadPackage: jest.fn(async () => {
        await sleep(10);
        throw new Error('CDN fetch exploded');
      }),
    });
    const cache = createMockPackageCacheService();
    const { ctrl, eventEmitter } = await buildController({
      packageCacheService: cache,
      cdnLoader: cdn,
      queryBuilder: qb,
      websocketGateway: ws,
    });

    const repo = createMockPackageRepo(qb);
    const req = makeReq({ name: 'bad-pkg', type: 'Server' }, repo);

    const result = await ctrl.installPackage(req);
    expect(result.data[0].status).toBe('installing');

    await sleep(50);

    const record = Array.from(qb.records.values()).find(
      (r: any) => r.name === 'bad-pkg',
    );
    expect(record.status).toBe('failed');
    expect(record.lastError).toContain('CDN fetch exploded');

    const failedEvents = ws.getEventsFor('failed');
    expect(failedEvents.length).toBe(1);
    expect(failedEvents[0].data.operation).toBe('install');
  });

  it('installs App packages synchronously with status=installed', async () => {
    const qb = createMockQueryBuilder();
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader();
    const cache = createMockPackageCacheService();
    const { ctrl, eventEmitter } = await buildController({
      packageCacheService: cache,
      cdnLoader: cdn,
      queryBuilder: qb,
      websocketGateway: ws,
    });

    const repo = createMockPackageRepo(qb);
    const req = makeReq(
      { name: 'vue-plugin', type: 'App', version: '2.0.0' },
      repo,
    );

    const result = await ctrl.installPackage(req);
    expect(result.data[0].status).toBe('installed');
    expect(result.data[0].type).toBe('App');
    expect(cache.reload).not.toHaveBeenCalled();
    expect(cdn.loadPackage).not.toHaveBeenCalled();
  });

  it('rejects duplicate packages', async () => {
    const qb = createMockQueryBuilder();
    qb.records.set(1, {
      id: 1,
      name: 'dup-pkg',
      type: 'Server',
      status: 'installed',
    });
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader();
    const cache = createMockPackageCacheService();
    const { ctrl, eventEmitter } = await buildController({
      packageCacheService: cache,
      cdnLoader: cdn,
      queryBuilder: qb,
      websocketGateway: ws,
    });

    const repo = createMockPackageRepo(qb);
    const req = makeReq({ name: 'dup-pkg', type: 'Server' }, repo);

    await expect(ctrl.installPackage(req)).rejects.toThrow(/already installed/);
  });

  it('validates required fields', async () => {
    const qb = createMockQueryBuilder();
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader();
    const cache = createMockPackageCacheService();
    const { ctrl, eventEmitter } = await buildController({
      packageCacheService: cache,
      cdnLoader: cdn,
      queryBuilder: qb,
      websocketGateway: ws,
    });

    const repo = createMockPackageRepo(qb);
    await expect(ctrl.installPackage(makeReq({}, repo))).rejects.toThrow(
      /name is required/,
    );
    await expect(
      ctrl.installPackage(makeReq({ name: 'foo' }, repo)),
    ).rejects.toThrow(/type is required/);
  });
});

describe('PackageController – update', () => {
  it('returns quickly with status=updating when Server version changes', async () => {
    const qb = createMockQueryBuilder();
    qb.records.set(1, {
      id: 1,
      name: 'test-pkg',
      type: 'Server',
      version: '1.0.0',
      status: 'installed',
    });
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader({
      invalidatePackage: jest.fn(async () => {
        await sleep(200);
      }),
    });
    const cache = createMockPackageCacheService();
    const { ctrl, eventEmitter } = await buildController({
      packageCacheService: cache,
      cdnLoader: cdn,
      queryBuilder: qb,
      websocketGateway: ws,
    });

    const repo = createMockPackageRepo(qb);
    const req = makeReq({ version: '2.0.0' }, repo);

    const t0 = Date.now();
    const result = await ctrl.updatePackage('1', req);
    const elapsed = Date.now() - t0;

    expect(elapsed).toBeLessThan(100);
    expect(qb.records.get(1).status).toBe('updating');

    const updatingEvents = ws.getEventsFor('updating');
    expect(updatingEvents.length).toBe(1);
    expect(updatingEvents[0].data.from).toBe('1.0.0');
    expect(updatingEvents[0].data.to).toBe('2.0.0');
    expect(result.data?.[0]?.version).toBe('1.0.0');
  });

  it('sets installed after executeCdnUpdate succeeds', async () => {
    const qb = createMockQueryBuilder();
    qb.records.set(1, {
      id: 1,
      name: 'test-pkg',
      type: 'Server',
      version: '1.0.0',
      status: 'installed',
    });
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader();
    const cache = createMockPackageCacheService();
    const { ctrl, eventEmitter } = await buildController({
      packageCacheService: cache,
      cdnLoader: cdn,
      queryBuilder: qb,
      websocketGateway: ws,
    });

    const repo = createMockPackageRepo(qb);
    const req = makeReq({ version: '2.0.0' }, repo);
    await ctrl.updatePackage('1', req);
    await sleep(30);

    expect(qb.records.get(1).status).toBe('installed');
    expect(qb.records.get(1).version).toBe('2.0.0');
    expect(qb.records.get(1).lastError).toBeNull();
    expect(cdn.invalidatePackage).toHaveBeenCalledWith('test-pkg', '2.0.0');
    expect(eventEmitter.emit).toHaveBeenCalled();

    const installedEvents = ws.getEventsFor('installed');
    expect(installedEvents.length).toBe(1);
  });

  it('sets failed when invalidatePackage throws', async () => {
    const qb = createMockQueryBuilder();
    qb.records.set(1, {
      id: 1,
      name: 'test-pkg',
      type: 'Server',
      version: '1.0.0',
      status: 'installed',
    });
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader({
      invalidatePackage: jest.fn(async () => {
        throw new Error('CDN update boom');
      }),
    });
    const cache = createMockPackageCacheService();
    const { ctrl, eventEmitter } = await buildController({
      packageCacheService: cache,
      cdnLoader: cdn,
      queryBuilder: qb,
      websocketGateway: ws,
    });

    const repo = createMockPackageRepo(qb);
    const req = makeReq({ version: '2.0.0' }, repo);
    await ctrl.updatePackage('1', req);
    await sleep(30);

    expect(qb.records.get(1).status).toBe('failed');
    expect(qb.records.get(1).lastError).toContain('CDN update boom');

    const failedEvents = ws.getEventsFor('failed');
    expect(failedEvents.length).toBe(1);
    expect(failedEvents[0].data.operation).toBe('update');
  });

  it('updates App packages synchronously', async () => {
    const qb = createMockQueryBuilder();
    qb.records.set(1, {
      id: 1,
      name: 'vue-plugin',
      type: 'App',
      version: '1.0.0',
      status: 'installed',
    });
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader();
    const cache = createMockPackageCacheService();
    const { ctrl, eventEmitter } = await buildController({
      packageCacheService: cache,
      cdnLoader: cdn,
      queryBuilder: qb,
      websocketGateway: ws,
    });

    const repo = createMockPackageRepo(qb);
    const req = makeReq({ version: '2.0.0' }, repo);
    await ctrl.updatePackage('1', req);

    expect(eventEmitter.emit).toHaveBeenCalledTimes(1);
    expect(cdn.invalidatePackage).not.toHaveBeenCalled();
  });

  it('updates non-version fields synchronously for Server', async () => {
    const qb = createMockQueryBuilder();
    qb.records.set(1, {
      id: 1,
      name: 'test-pkg',
      type: 'Server',
      version: '1.0.0',
      status: 'installed',
      description: 'old',
    });
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader();
    const cache = createMockPackageCacheService();
    const { ctrl, eventEmitter } = await buildController({
      packageCacheService: cache,
      cdnLoader: cdn,
      queryBuilder: qb,
      websocketGateway: ws,
    });

    const repo = createMockPackageRepo(qb);
    const req = makeReq({ description: 'new desc' }, repo);
    await ctrl.updatePackage('1', req);

    expect(cdn.invalidatePackage).not.toHaveBeenCalled();
    expect(eventEmitter.emit).toHaveBeenCalledTimes(1);
  });
});

describe('PackageController – uninstall', () => {
  it('deletes Server package, invalidates CDN, reloads, emits uninstalled', async () => {
    const qb = createMockQueryBuilder();
    qb.records.set(1, {
      id: 1,
      name: 'test-pkg',
      type: 'Server',
      version: '1.0.0',
      status: 'installed',
      isSystem: false,
    });
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader();
    const cache = createMockPackageCacheService();
    const { ctrl, eventEmitter } = await buildController({
      packageCacheService: cache,
      cdnLoader: cdn,
      queryBuilder: qb,
      websocketGateway: ws,
    });

    const repo = createMockPackageRepo(qb);
    const req = makeReq({}, repo);
    await ctrl.uninstallPackage('1', req);

    expect(qb.records.has(1)).toBe(false);
    expect(cdn.invalidatePackage).toHaveBeenCalledWith('test-pkg');
    expect(eventEmitter.emit).toHaveBeenCalled();

    const events = ws.getEventsFor('uninstalled');
    expect(events.length).toBe(1);
    expect(events[0].data.name).toBe('test-pkg');
  });

  it('rejects uninstalling system packages', async () => {
    const qb = createMockQueryBuilder();
    qb.records.set(1, {
      id: 1,
      name: 'core-pkg',
      type: 'Server',
      isSystem: true,
      status: 'installed',
    });
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader();
    const cache = createMockPackageCacheService();
    const { ctrl, eventEmitter } = await buildController({
      packageCacheService: cache,
      cdnLoader: cdn,
      queryBuilder: qb,
      websocketGateway: ws,
    });

    const repo = createMockPackageRepo(qb);
    const req = makeReq({}, repo);
    await expect(ctrl.uninstallPackage('1', req)).rejects.toThrow(
      /Cannot uninstall system packages/,
    );
  });

  it('deletes App packages without CDN invalidate', async () => {
    const qb = createMockQueryBuilder();
    qb.records.set(1, {
      id: 1,
      name: 'vue-plugin',
      type: 'App',
      isSystem: false,
      status: 'installed',
    });
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader();
    const cache = createMockPackageCacheService();
    const { ctrl, eventEmitter } = await buildController({
      packageCacheService: cache,
      cdnLoader: cdn,
      queryBuilder: qb,
      websocketGateway: ws,
    });

    const repo = createMockPackageRepo(qb);
    const req = makeReq({}, repo);
    await ctrl.uninstallPackage('1', req);

    expect(qb.records.has(1)).toBe(false);
    expect(eventEmitter.emit).toHaveBeenCalledTimes(1);
    expect(cdn.invalidatePackage).not.toHaveBeenCalled();
  });
});

describe('PackageCacheService – preloadPackagesFromCdn', () => {
  it('loads installed Server packages not yet in CDN cache', async () => {
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader();

    const { service, qb } = await buildCacheService({
      dbPackages: [
        {
          id: 1,
          name: 'dayjs',
          version: '1.11.0',
          type: 'Server',
          isEnabled: true,
          status: 'installed',
        },
        {
          id: 2,
          name: 'zod',
          version: '3.0.0',
          type: 'Server',
          isEnabled: true,
          status: 'installed',
        },
      ],
      cdnLoader: cdn,
      ws,
    });

    await service['preloadPackagesFromCdn']();

    expect(cdn.loadPackage).toHaveBeenCalledWith('dayjs', '1.11.0');
    expect(cdn.loadPackage).toHaveBeenCalledWith('zod', '3.0.0');
    expect(qb.records.get(1).status).toBe('installed');
    expect(qb.records.get(2).status).toBe('installed');

    const installingEvents = ws.getEventsFor('installing');
    expect(installingEvents.length).toBe(1);
    expect(installingEvents[0].data.packages).toHaveLength(2);

    const installedEvents = ws.getEventsFor('installed');
    expect(installedEvents.length).toBe(2);
  });

  it('marks failed when loadPackage throws', async () => {
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader({
      loadPackage: jest.fn(async (name: string) => {
        if (name === 'bad') throw new Error('CDN error');
        return {};
      }),
    });

    const { service, qb } = await buildCacheService({
      dbPackages: [
        {
          id: 1,
          name: 'good',
          version: '1.0.0',
          type: 'Server',
          isEnabled: true,
          status: 'installed',
        },
        {
          id: 2,
          name: 'bad',
          version: '1.0.0',
          type: 'Server',
          isEnabled: true,
          status: 'installed',
        },
      ],
      cdnLoader: cdn,
      ws,
    });

    await service['preloadPackagesFromCdn']();

    expect(qb.records.get(1).status).toBe('installed');
    expect(qb.records.get(2).status).toBe('failed');
    expect(qb.records.get(2).lastError).toContain('CDN error');

    const failedEvents = ws.getEventsFor('failed');
    expect(failedEvents.some((e) => e.data.operation === 'preload')).toBe(true);
  });

  it('skips packages already loaded in CDN loader', async () => {
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader();
    cdn.loaded.add('dayjs');
    cdn.isLoaded.mockImplementation((name: string) => cdn.loaded.has(name));

    const { service } = await buildCacheService({
      dbPackages: [
        {
          id: 1,
          name: 'dayjs',
          version: '1.11.0',
          type: 'Server',
          isEnabled: true,
          status: 'installed',
        },
      ],
      cdnLoader: cdn,
      ws,
    });

    await service['preloadPackagesFromCdn']();

    expect(cdn.loadPackage).not.toHaveBeenCalled();
    expect(ws.getEventsFor('installing').length).toBe(0);
  });
});

describe('extractErrorMessage – cause chain', () => {
  let extractErrorMessage: (error: any) => string;

  beforeAll(async () => {
    const mod =
      await import('../../src/infrastructure/cache/services/package-cdn-loader.service');
    extractErrorMessage = mod.extractErrorMessage;
  });

  it('returns simple message for plain errors', () => {
    expect(extractErrorMessage(new Error('boom'))).toBe('boom');
  });

  it('walks nested cause chain', () => {
    const root = new Error('connect failed');
    const mid = new Error('fetch failed', { cause: root });
    const top = new Error('CDN load failed', { cause: mid });

    expect(extractErrorMessage(top)).toBe(
      'CDN load failed → fetch failed → connect failed',
    );
  });

  it('returns Unknown error for empty error', () => {
    expect(extractErrorMessage({})).toBe('Unknown error');
    expect(extractErrorMessage(null)).toBe('Unknown error');
  });

  it('handles real Node.js fetch-style TypeError with cause', () => {
    const cause = new Error('getaddrinfo ENOTFOUND esm.sh');
    const fetchError = new TypeError('fetch failed', { cause });

    expect(extractErrorMessage(fetchError)).toBe(
      'fetch failed → getaddrinfo ENOTFOUND esm.sh',
    );
  });
});

describe('PackageCacheService – retry failed packages on startup', () => {
  it('retries packages with status=failed during preload', async () => {
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader();

    const { service, qb } = await buildCacheService({
      dbPackages: [
        {
          id: 1,
          name: 'ms',
          version: '2.1.3',
          type: 'Server',
          isEnabled: true,
          status: 'failed',
        },
        {
          id: 2,
          name: 'dayjs',
          version: '1.11.13',
          type: 'Server',
          isEnabled: true,
          status: 'failed',
        },
      ],
      cdnLoader: cdn,
      ws,
    });

    await service['preloadPackagesFromCdn']();

    expect(cdn.loadPackage).toHaveBeenCalledWith('ms', '2.1.3');
    expect(cdn.loadPackage).toHaveBeenCalledWith('dayjs', '1.11.13');
    expect(qb.records.get(1).status).toBe('installed');
    expect(qb.records.get(1).lastError).toBeNull();
    expect(qb.records.get(2).status).toBe('installed');
    expect(qb.records.get(2).lastError).toBeNull();
  });

  it('retries failed packages alongside installed ones', async () => {
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader();

    const { service, qb } = await buildCacheService({
      dbPackages: [
        {
          id: 1,
          name: 'ms',
          version: '2.1.3',
          type: 'Server',
          isEnabled: true,
          status: 'failed',
        },
        {
          id: 2,
          name: 'zod',
          version: '3.0.0',
          type: 'Server',
          isEnabled: true,
          status: 'installed',
        },
      ],
      cdnLoader: cdn,
      ws,
    });

    await service['preloadPackagesFromCdn']();

    expect(cdn.loadPackage).toHaveBeenCalledTimes(2);
    expect(qb.records.get(1).status).toBe('installed');
    expect(qb.records.get(2).status).toBe('installed');
  });

  it('keeps failed status if retry also fails', async () => {
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader({
      loadPackage: jest.fn(async () => {
        throw new Error('still broken', { cause: new Error('ENOTFOUND') });
      }),
    });

    const { service, qb } = await buildCacheService({
      dbPackages: [
        {
          id: 1,
          name: 'ms',
          version: '2.1.3',
          type: 'Server',
          isEnabled: true,
          status: 'failed',
        },
      ],
      cdnLoader: cdn,
      ws,
    });

    await service['preloadPackagesFromCdn']();

    expect(qb.records.get(1).status).toBe('failed');
    expect(qb.records.get(1).lastError).toContain('still broken');
    expect(qb.records.get(1).lastError).toContain('ENOTFOUND');
  });

  it('does not retry packages with status=installing or updating', async () => {
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader();

    const { service } = await buildCacheService({
      dbPackages: [
        {
          id: 1,
          name: 'ms',
          version: '2.1.3',
          type: 'Server',
          isEnabled: true,
          status: 'installing',
        },
        {
          id: 2,
          name: 'dayjs',
          version: '1.11.13',
          type: 'Server',
          isEnabled: true,
          status: 'updating',
        },
      ],
      cdnLoader: cdn,
      ws,
    });

    await service['preloadPackagesFromCdn']();

    expect(cdn.loadPackage).not.toHaveBeenCalled();
  });
});

describe('PackageController – error cause chain in lastError', () => {
  it('stores full cause chain in lastError on install failure', async () => {
    const qb = createMockQueryBuilder();
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader({
      loadPackage: jest.fn(async () => {
        await sleep(10);
        throw new TypeError('fetch failed', {
          cause: new Error('getaddrinfo ENOTFOUND esm.sh'),
        });
      }),
    });
    const cache = createMockPackageCacheService();
    const { ctrl, eventEmitter } = await buildController({
      packageCacheService: cache,
      cdnLoader: cdn,
      queryBuilder: qb,
      websocketGateway: ws,
    });

    const repo = createMockPackageRepo(qb);
    const req = makeReq({ name: 'fail-pkg', type: 'Server' }, repo);
    await ctrl.installPackage(req);
    await sleep(50);

    const record = Array.from(qb.records.values()).find(
      (r: any) => r.name === 'fail-pkg',
    );
    expect(record.status).toBe('failed');
    expect(record.lastError).toContain('fetch failed');
    expect(record.lastError).toContain('ENOTFOUND');
    expect(record.lastError).toContain('→');
  });

  it('stores full cause chain in lastError on update failure', async () => {
    const qb = createMockQueryBuilder();
    qb.records.set(1, {
      id: 1,
      name: 'test-pkg',
      type: 'Server',
      version: '1.0.0',
      status: 'installed',
    });
    const ws = createMockWebSocketGateway();
    const cdn = createMockCdnLoader({
      invalidatePackage: jest.fn(async () => {
        throw new Error('network timeout', {
          cause: new Error('ETIMEDOUT 1.2.3.4:443'),
        });
      }),
    });
    const cache = createMockPackageCacheService();
    const { ctrl, eventEmitter } = await buildController({
      packageCacheService: cache,
      cdnLoader: cdn,
      queryBuilder: qb,
      websocketGateway: ws,
    });

    const repo = createMockPackageRepo(qb);
    const req = makeReq({ version: '2.0.0' }, repo);
    await ctrl.updatePackage('1', req);
    await sleep(50);

    expect(qb.records.get(1).status).toBe('failed');
    expect(qb.records.get(1).lastError).toContain('network timeout');
    expect(qb.records.get(1).lastError).toContain('ETIMEDOUT');
  });
});

describe('WS event naming ($system:package:*)', () => {
  it('all emitted events use $system:package: prefix and emit to /enfyra-admin', async () => {
    const qb = createMockQueryBuilder();
    const ws = createMockWebSocketGateway();
    const installDone = (() => {
      let r!: () => void;
      const p = new Promise<void>((res) => {
        r = res;
      });
      return { promise: p, resolve: r };
    })();
    const cdn = createMockCdnLoader({
      loadPackage: jest.fn(async () => {
        throw new Error('fail for event test');
      }),
    });
    const cache = createMockPackageCacheService();
    const { ctrl, eventEmitter } = await buildController({
      packageCacheService: cache,
      cdnLoader: cdn,
      queryBuilder: qb,
      websocketGateway: ws,
    });

    const origExecute = ctrl['executeCdnLoad'].bind(ctrl);
    ctrl['executeCdnLoad'] = async (...args: any[]) => {
      try {
        await origExecute(...args);
      } finally {
        installDone.resolve();
      }
    };

    const repo = createMockPackageRepo(qb);
    await ctrl.installPackage(
      makeReq({ name: 'evt-test', type: 'Server' }, repo),
    );
    await installDone.promise;

    for (const evt of ws.events) {
      expect(evt.event).toMatch(/^\$system:package:/);
    }

    expect(ws.emitToNamespace).toHaveBeenCalledWith(
      '/enfyra-admin',
      expect.stringMatching(/^\$system:package:/),
      expect.any(Object),
    );
  });
});
