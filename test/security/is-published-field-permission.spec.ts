import { ForbiddenException } from '@nestjs/common';
import { DynamicRepository } from '../../src/modules/dynamic-api/repositories/dynamic.repository';
import { matchFieldPermissionCondition } from '../../src/shared/utils/field-permission-condition.util';


type TPolicy = {
  unconditionalAllowedColumns: Set<string>;
  unconditionalAllowedRelations: Set<string>;
  unconditionalDeniedColumns: Set<string>;
  unconditionalDeniedRelations: Set<string>;
  rules: any[];
};

function makePolicy(partial?: Partial<TPolicy>): TPolicy {
  return {
    unconditionalAllowedColumns: new Set<string>(),
    unconditionalAllowedRelations: new Set<string>(),
    unconditionalDeniedColumns: new Set<string>(),
    unconditionalDeniedRelations: new Set<string>(),
    rules: [],
    ...(partial ?? {}),
  };
}

function makeRule(opts: {
  id: number;
  action: 'read' | 'create' | 'update';
  effect: 'allow' | 'deny';
  tableName: string;
  roleId?: string | null;
  allowedUserIds?: string[];
  columnName?: string | null;
  relationPropertyName?: string | null;
  condition?: any | null;
}) {
  return {
    id: opts.id,
    isEnabled: true,
    action: opts.action,
    effect: opts.effect,
    tableName: opts.tableName,
    roleId: 'roleId' in opts ? opts.roleId : null,
    allowedUserIds: opts.allowedUserIds ?? [],
    columnName: 'columnName' in opts ? opts.columnName : null,
    relationPropertyName:
      'relationPropertyName' in opts ? opts.relationPropertyName : null,
    condition: 'condition' in opts ? opts.condition : null,
  };
}

function makeRepoHarness(opts: {
  isMongo?: boolean;
  enforce?: boolean;
  user?: any;
  query?: any;
  policies?: any[];
  tableMeta: any;
  dataFixture?: any[];
}) {
  const tableName = opts.tableMeta.name;

  const queryEngine = {
    find: jest.fn(async (findOpts: any) => {
      let data = JSON.parse(JSON.stringify(opts.dataFixture ?? []));
      const f = findOpts?.fields;
      if (f && f !== '' && f !== '*') {
        const allowed = new Set(String(f).split(',').map((s: string) => s.split('.')[0].trim()));
        data = data.map((row: any) => {
          const out: any = {};
          for (const key of Object.keys(row)) {
            if (allowed.has(key)) out[key] = row[key];
          }
          return out;
        });
      }
      const deepOpts = findOpts?.deep;
      if (deepOpts && typeof deepOpts === 'object') {
        for (const row of data) {
          for (const relName of Object.keys(deepOpts)) {
            if (!row[relName] || !deepOpts[relName]?.fields) continue;
            const relFields = new Set(String(deepOpts[relName].fields).split(',').map((s: string) => s.trim()));
            const nested = row[relName];
            if (nested && typeof nested === 'object' && !Array.isArray(nested)) {
              const cleaned: any = {};
              for (const k of Object.keys(nested)) { if (relFields.has(k)) cleaned[k] = nested[k]; }
              row[relName] = cleaned;
            }
          }
        }
      }
      return { data, meta: { totalCount: (opts.dataFixture ?? []).length } };
    }),
  } as any;

  const metadataCacheService = {
    lookupTableByName: jest.fn(async () => opts.tableMeta),
    getDirectMetadata: jest.fn(() => ({
      tables: new Map([[tableName, opts.tableMeta]]),
    })),
  } as any;

  const fieldPermissionCacheService = {
    getPoliciesFor: jest.fn(async () => opts.policies ?? []),
  } as any;

  const repo = new DynamicRepository({
    context: {
      $query: opts.query ?? {},
      $user: opts.user ?? { id: 'u1', role: { id: 'r1' } },
    } as any,
    tableName,
    queryEngine,
    queryBuilder: {
      isMongoDb: () => opts.isMongo === true,
      runWithPolicy: async (_cb: any, fn: any) => await fn(),
      runWithFieldPermissionCheck: async (_cb: any, fn: any) => await fn(),
      insertAndGet: async () => ({ id: 1 }),
      updateById: async () => ({ id: 1 }),
      deleteById: async () => ({ id: 1 }),
    } as any,
    tableHandlerService: {
      createTable: jest.fn(),
    } as any,
    policyService: { checkMutationSafety: jest.fn(async () => ({ allow: true })) } as any,
    tableValidationService: { assertTableValid: jest.fn(async () => {}) } as any,
    metadataCacheService,
    settingCacheService: { getMaxQueryDepth: () => 7 } as any,
    eventEmitter: { emit: jest.fn() } as any,
    fieldPermissionCacheService,
    enforceFieldPermission: opts.enforce === true,
  });

  return {
    repo,
    queryEngine,
    metadataCacheService,
    fieldPermissionCacheService,
  };
}

describe('field permissions (isPublished baseline + overrides)', () => {
  const tableName = 'tasks';
  const usersTable = 'users';

  const baseMeta = {
    name: tableName,
    columns: [
      { name: 'id', isPublished: true },
      { name: 'title', isPublished: true },
      { name: 'ownerId', isPublished: true },
      { name: 'secretNote', isPublished: false },
      { name: 'internalCode', isPublished: true },
    ],
    relations: [
      { propertyName: 'owner', targetTableName: usersTable, isPublished: false },
    ],
  };

  const usersMeta = {
    name: usersTable,
    columns: [
      { name: 'id', isPublished: true },
      { name: 'email', isPublished: false },
    ],
    relations: [],
  };

  function makeMetadataMap() {
    return {
      tables: new Map([
        [tableName, baseMeta],
        [usersTable, usersMeta],
      ]),
    };
  }

  function withMetadataMap(h: any) {
    const map = makeMetadataMap();
    h.metadataCacheService.getDirectMetadata = jest.fn(() => map);
    h.metadataCacheService.lookupTableByName = jest.fn(async (name: string) => map.tables.get(name) || null);
    return h;
  }

  test('read shaping: unpublished => null; allow overrides (public/role/user/condition); rootAdmin bypass', async () => {
    const conditionalSelfPolicy = makePolicy({
      rules: [
        makeRule({
          id: 4,
          action: 'read',
          effect: 'allow',
          tableName,
          roleId: null,
          columnName: 'secretNote',
          condition: { ownerId: { _eq: '@USER.id' } },
        }),
      ],
    });

    const publicAllowPolicy = makePolicy({
      unconditionalAllowedColumns: new Set(['secretNote']),
      rules: [
        makeRule({
          id: 1,
          action: 'read',
          effect: 'allow',
          tableName,
          roleId: null,
          columnName: 'secretNote',
        }),
      ],
    });

    const roleAllowPolicy = makePolicy({
      unconditionalAllowedColumns: new Set(['secretNote']),
      rules: [
        makeRule({
          id: 2,
          action: 'read',
          effect: 'allow',
          tableName,
          roleId: 'r_manager',
          columnName: 'secretNote',
        }),
      ],
    });

    const userAllowPolicy = makePolicy({
      rules: [
        makeRule({
          id: 3,
          action: 'read',
          effect: 'allow',
          tableName,
          allowedUserIds: ['u_allow'],
          columnName: 'secretNote',
        }),
      ],
    });

    const cases: Array<{
      name: string;
      user: any;
      policies: any[];
      fixture: any[];
      expectValue: any;
    }> = [
      {
        name: 'anon default => null',
        user: { isAnonymous: true },
        policies: [],
        fixture: [{ id: 1, secretNote: 'x' }],
        expectValue: undefined,
      },
      {
        name: 'auth default => null',
        user: { id: 'u1', role: { id: 'r1' } },
        policies: [],
        fixture: [{ id: 1, secretNote: 'x' }],
        expectValue: undefined,
      },
      {
        name: 'public allow => value',
        user: { isAnonymous: true },
        policies: [publicAllowPolicy],
        fixture: [{ id: 1, secretNote: 'x' }],
        expectValue: 'x',
      },
      {
        name: 'role allow => value',
        user: { id: 'u1', role: { id: 'r_manager' } },
        policies: [roleAllowPolicy],
        fixture: [{ id: 1, secretNote: 'x' }],
        expectValue: 'x',
      },
      {
        name: 'allowedUsers allow => value',
        user: { id: 'u_allow', role: { id: 'r1' } },
        policies: [userAllowPolicy],
        fixture: [{ id: 1, secretNote: 'x' }],
        expectValue: 'x',
      },
      {
        name: 'conditional self allow => mixed',
        user: { id: 'u1', role: { id: 'r1' } },
        policies: [conditionalSelfPolicy],
        fixture: [
          { id: 1, ownerId: 'u1', secretNote: 'mine' },
          { id: 2, ownerId: 'u2', secretNote: 'other' },
        ],
        expectValue: ['mine', undefined],
      },
      {
        name: 'rootAdmin bypass => value',
        user: { id: 'root', isRootAdmin: true, role: { id: 'r_root' } },
        policies: [],
        fixture: [{ id: 1, secretNote: 'x' }],
        expectValue: 'x',
      },
    ];

    for (const c of cases) {
      const h = withMetadataMap(
        makeRepoHarness({
          enforce: true,
          user: c.user,
          query: { fields: 'id,ownerId,secretNote' },
          tableMeta: baseMeta,
          policies: c.policies,
          dataFixture: c.fixture,
        }),
      );
      const res = await h.repo.find();
      if (Array.isArray(c.expectValue)) {
        expect(res.data.map((r: any) => r.secretNote)).toEqual(c.expectValue);
      } else {
        expect(res.data[0].secretNote).toBe(c.expectValue);
      }
    }
  });

  test('relation deep shaping: unpublished relation => null; allow override works; nested unpublished column => null', async () => {
    const relAllowPolicy = makePolicy({
      unconditionalAllowedRelations: new Set(['owner']),
      rules: [
        makeRule({
          id: 10,
          action: 'read',
          effect: 'allow',
          tableName,
          roleId: null,
          relationPropertyName: 'owner',
        }),
      ],
    });

    const h1 = withMetadataMap(
      makeRepoHarness({
        enforce: true,
        query: { deep: { owner: { fields: 'id,email' } } },
        tableMeta: baseMeta,
        policies: [],
        dataFixture: [{ id: 1, owner: { id: 'u1', email: 'a@a.com' } }],
      }),
    );
    const r1 = await h1.repo.find();
    expect(r1.data[0].owner).toBeUndefined();

    const h2 = withMetadataMap(
      makeRepoHarness({
        enforce: true,
        user: { isAnonymous: true },
        query: { deep: { owner: { fields: 'id,email' } } },
        tableMeta: baseMeta,
        policies: [relAllowPolicy],
        dataFixture: [{ id: 1, owner: { id: 'u1', email: 'a@a.com' } }],
      }),
    );
    const r2 = await h2.repo.find();
    expect(r2.data[0].owner?.id).toBe('u1');
    expect(r2.data[0].owner?.email).toBeUndefined();
  });

  test('query operators: unpublished field => 403 unless unconditional allow; select is never 403; rootAdmin bypass', async () => {
    const conditionalAllowPolicy = makePolicy({
      rules: [
        makeRule({
          id: 16,
          action: 'read',
          effect: 'allow',
          tableName,
          roleId: null,
          columnName: 'secretNote',
          condition: { ownerId: { _eq: '@USER.id' } },
        }),
      ],
    });

    const unconditionalAllowPolicy = makePolicy({
      unconditionalAllowedColumns: new Set(['secretNote']),
      rules: [
        makeRule({
          id: 17,
          action: 'read',
          effect: 'allow',
          tableName,
          roleId: null,
          columnName: 'secretNote',
        }),
      ],
    });

    const operators = [
      { query: { filter: { secretNote: { _eq: 'x' } } } },
      { query: { sort: 'secretNote' } },
      { query: { aggregate: { count: { secretNote: true } } } },
    ];

    for (const op of operators) {
      const h = withMetadataMap(
        makeRepoHarness({
          enforce: true,
          query: op.query,
          tableMeta: baseMeta,
          policies: [],
          dataFixture: [],
        }),
      );
      await expect(h.repo.find()).rejects.toBeInstanceOf(ForbiddenException);

      const hCond = withMetadataMap(
        makeRepoHarness({
          enforce: true,
          query: op.query,
          tableMeta: baseMeta,
          policies: [conditionalAllowPolicy],
          dataFixture: [],
        }),
      );
      await expect(hCond.repo.find()).rejects.toBeInstanceOf(ForbiddenException);

      const hAllow = withMetadataMap(
        makeRepoHarness({
          enforce: true,
          query: op.query,
          tableMeta: baseMeta,
          policies: [unconditionalAllowPolicy],
          dataFixture: [],
        }),
      );
      await expect(hAllow.repo.find()).resolves.toBeTruthy();
    }

    const hSelectOnly = withMetadataMap(
      makeRepoHarness({
        enforce: true,
        query: { fields: 'id,secretNote' },
        tableMeta: baseMeta,
        policies: [],
        dataFixture: [{ id: 1, secretNote: 'x' }],
      }),
    );
    const rSelectOnly = await hSelectOnly.repo.find();
    expect(rSelectOnly.data[0].secretNote).toBeUndefined();

    const hFilterPublishedSelectUnpub = withMetadataMap(
      makeRepoHarness({
        enforce: true,
        query: { filter: { title: { _eq: 't' } }, fields: 'id,secretNote,title' },
        tableMeta: baseMeta,
        policies: [],
        dataFixture: [{ id: 1, title: 't', secretNote: 'x' }],
      }),
    );
    const r2 = await hFilterPublishedSelectUnpub.repo.find();
    expect(r2.data[0].title).toBe('t');
    expect(r2.data[0].secretNote).toBeUndefined();

    const hRoot = withMetadataMap(
      makeRepoHarness({
        enforce: true,
        user: { id: 'root', isRootAdmin: true, role: { id: 'r_root' } },
        query: { filter: { secretNote: { _eq: 'x' } } },
        tableMeta: baseMeta,
        policies: [],
        dataFixture: [],
      }),
    );
    await expect(hRoot.repo.find()).resolves.toBeTruthy();
  });

  test('write enforcement: unpublished blocked by default, allow override works, deny can block published', async () => {
    const hBase = withMetadataMap(
      makeRepoHarness({
        enforce: true,
        query: {},
        tableMeta: baseMeta,
        policies: [],
        dataFixture: [{ id: 1, title: 't' }],
      }),
    );
    await expect(
      hBase.repo.create({ data: { title: 't', secretNote: 'x' } }),
    ).rejects.toBeInstanceOf(ForbiddenException);
    await expect(
      hBase.repo.update({ id: 1, data: { secretNote: 'x' } }),
    ).rejects.toBeInstanceOf(ForbiddenException);

    const allowCreatePolicy = makePolicy({
      rules: [
        makeRule({
          id: 22,
          action: 'create',
          effect: 'allow',
          tableName,
          roleId: null,
          columnName: 'secretNote',
        }),
      ],
    });
    const hAllowCreate = withMetadataMap(
      makeRepoHarness({
        enforce: true,
        query: {},
        tableMeta: baseMeta,
        policies: [allowCreatePolicy],
        dataFixture: [],
      }),
    );
    await expect(
      hAllowCreate.repo.create({ data: { title: 't', secretNote: 'x' } }),
    ).resolves.toBeTruthy();

    const allowUpdatePolicy = makePolicy({
      rules: [
        makeRule({
          id: 23,
          action: 'update',
          effect: 'allow',
          tableName,
          roleId: null,
          columnName: 'secretNote',
        }),
      ],
    });
    const hAllowUpdate = withMetadataMap(
      makeRepoHarness({
        enforce: true,
        query: {},
        tableMeta: baseMeta,
        policies: [allowUpdatePolicy],
        dataFixture: [{ id: 1, title: 't', secretNote: null }],
      }),
    );
    await expect(
      hAllowUpdate.repo.update({ id: 1, data: { secretNote: 'x' } }),
    ).resolves.toBeTruthy();

    const denyPublishedPolicy = makePolicy({
      rules: [
        makeRule({
          id: 30,
          action: 'update',
          effect: 'deny',
          tableName,
          roleId: null,
          columnName: 'title',
        }),
      ],
    });
    const hDeny = withMetadataMap(
      makeRepoHarness({
        enforce: true,
        query: {},
        tableMeta: baseMeta,
        policies: [denyPublishedPolicy],
        dataFixture: [{ id: 1, title: 't' }],
      }),
    );
    await expect(hDeny.repo.update({ id: 1, data: { title: 'x' } })).rejects.toBeInstanceOf(
      ForbiddenException,
    );
  });

  test('enforcement scope: enforce=false bypasses sanitize + 403', async () => {
    const h = withMetadataMap(
      makeRepoHarness({
        enforce: false,
        query: { fields: 'id,secretNote', filter: { secretNote: { _eq: 'x' } } },
        tableMeta: baseMeta,
        policies: [],
        dataFixture: [{ id: 1, secretNote: 'x' }],
      }),
    );
    const res = await h.repo.find();
    expect(res.data[0].secretNote).toBe('x');
  });

  test('policy invariants: subject XOR + scope required', async () => {
    const { PolicyService } = await import('../../src/core/policy/policy.service');
    const policy = new PolicyService(
      { assertNoSystemFlagDeep: jest.fn() } as any,
      {
        getMetadata: jest.fn(async () => ({ tables: new Map() })),
      } as any,
    );
    const bad1 = await policy.checkMutationSafety({
      operation: 'create',
      tableName: 'field_permission_definition',
      data: { column: { id: 1 }, relation: { id: 2 }, role: { id: 1 } },
      existing: null,
      currentUser: { id: 'u1' },
    });
    expect(bad1.allow).toBe(false);
    if (bad1.allow === false) expect(bad1.message).toContain('exactly one of');

    const bad2 = await policy.checkMutationSafety({
      operation: 'create',
      tableName: 'field_permission_definition',
      data: { column: { id: 1 } },
      existing: null,
      currentUser: { id: 'u1' },
    });
    expect(bad2.allow).toBe(false);
    if (bad2.allow === false) expect(bad2.message).toContain('requires scope');
  });
});

