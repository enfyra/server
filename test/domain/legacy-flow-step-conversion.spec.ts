import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { DatabaseConfigService } from '../../src/shared/services';
import { convertLegacyFlowSteps } from '../../src/domain/bootstrap/utils/legacy-flow-step-conversion.util';

function makeSqlQueryBuilder(
  rows: any[],
  options: {
    hasTable?: boolean;
    hasSourceCode?: boolean;
    hasConfig?: boolean;
  } = {},
) {
  const updates: { id: any; data: any }[] = [];
  const builder: any = (table: string) => {
    expect(table).toBe('enfyra_flow_step');
    return {
      whereNotNull: () => ({
        select: async (columns: string[]) => {
          expect(columns).toContain('config');
          return rows;
        },
        count: async () => [{ count: rows.length }],
      }),
      where: (column: string, id: any) => ({
        update: async (data: any) => {
          expect(column).toBe('id');
          updates.push({ id, data });
        },
      }),
    };
  };
  builder.schema = {
    hasTable: async () => options.hasTable !== false,
    hasColumn: async (_table: string, column: string) =>
      column === 'config'
        ? options.hasConfig !== false
        : options.hasSourceCode !== false,
  };
  return {
    updates,
    getKnex: () => builder,
    getMongoDb: () => null,
  };
}

function makeMongoQueryBuilder(
  rows: any[],
  options: { hasCollection?: boolean } = {},
) {
  const updates: { id: any; set: any; unset: any }[] = [];
  const collection = {
    find: (filter: any) => {
      expect(filter).toEqual({ config: { $exists: true } });
      return { project: () => ({ toArray: async () => rows }) };
    },
    updateOne: async (filter: any, patch: any) => {
      updates.push({ id: filter._id, set: patch.$set, unset: patch.$unset });
    },
  };
  const db = {
    listCollections: () => ({
      toArray: async () =>
        options.hasCollection === false ? [] : [{ name: 'enfyra_flow_step' }],
    }),
    collection: () => collection,
  };
  return { updates, getKnex: () => null, getMongoDb: () => db };
}

const legacyRows = [
  { id: 1, key: 'wait', type: 'sleep', config: { ms: 5000 } },
  { id: 2, key: 'note', type: 'log', config: { message: 'done' } },
  {
    id: 3,
    key: 'users',
    type: 'query',
    config: { table: 'enfyra_user', filter: { id: { _eq: 1 } }, limit: 1 },
  },
  {
    id: 4,
    key: 'create_todo',
    type: 'create',
    config: { table: 'todo', data: { title: 'Review' } },
  },
  {
    id: 5,
    key: 'update_todo',
    type: 'update',
    config: { table: 'todo', id: 9, data: { status: 'done' } },
  },
  {
    id: 6,
    key: 'delete_todo',
    type: 'delete',
    config: { table: 'todo', id: 9 },
  },
  {
    id: 7,
    key: 'fan_out',
    type: 'trigger_flow',
    config: { flowName: 'child-flow', payload: { ok: true } },
  },
];

describe('convertLegacyFlowSteps', () => {
  beforeEach(() => {
    DatabaseConfigService.overrideForTesting('postgres');
  });

  afterEach(() => {
    DatabaseConfigService.resetForTesting();
  });

  it('converts each retired fixed step into an equivalent script step', async () => {
    const qb = makeSqlQueryBuilder(legacyRows);

    const count = await convertLegacyFlowSteps(qb as any);

    expect(count).toBe(7);
    const byId = new Map(qb.updates.map((entry) => [entry.id, entry.data]));
    expect(byId.get(1).sourceCode).toContain('const totalMs = 5000');
    expect(byId.get(1).sourceCode).toContain(
      'await @HELPERS.$sleep(Math.min(remaining, 30000))',
    );
    expect(byId.get(2).sourceCode).toContain('@LOGS(message)');
    expect(byId.get(3).sourceCode).toContain('#enfyra_user.find(');
    expect(byId.get(4).sourceCode).toContain('#todo.create(');
    expect(byId.get(5).sourceCode).toContain('#todo.update(');
    expect(byId.get(6).sourceCode).toContain('#todo.delete(');
    expect(byId.get(7).sourceCode).toBe(
      'return await @TRIGGER("child-flow", {"ok":true})',
    );
    for (const entry of qb.updates) {
      expect(entry.data).toMatchObject({
        type: 'script',
        scriptLanguage: 'javascript',
        config: null,
      });
      expect(typeof entry.data.compiledCode).toBe('string');
    }
  });

  it('backfills sourceCode from config for script steps that never had it', async () => {
    const qb = makeSqlQueryBuilder([
      {
        id: 8,
        key: 'legacy_code',
        type: 'script',
        sourceCode: null,
        config: {
          code: 'const a: number = 1; return a;',
          scriptLanguage: 'typescript',
        },
      },
    ]);

    await expect(convertLegacyFlowSteps(qb as any)).resolves.toBe(1);

    const update = qb.updates[0];
    expect(update.data.type).toBeUndefined();
    expect(update.data.scriptLanguage).toBe('typescript');
    expect(update.data.sourceCode).toContain('const a: number = 1;');
    expect(update.data.compiledCode).toContain('const a = 1;');
    expect(update.data.config).toBeNull();
  });

  it('leaves script steps that already have sourceCode untouched', async () => {
    const qb = makeSqlQueryBuilder([
      {
        id: 9,
        key: 'current',
        type: 'script',
        sourceCode: 'return 1;',
        config: {},
      },
    ]);

    await expect(convertLegacyFlowSteps(qb as any)).resolves.toBe(0);
    expect(qb.updates).toHaveLength(0);
  });

  it('uses the same conversion on MongoDB with _id and $unset', async () => {
    DatabaseConfigService.overrideForTesting('mongodb');
    const rows = [
      { _id: 'abc', key: 'wait', type: 'sleep', config: { ms: 1000 } },
    ];
    const qb = makeMongoQueryBuilder(rows);

    await expect(convertLegacyFlowSteps(qb as any)).resolves.toBe(1);

    expect(qb.updates[0].id).toBe('abc');
    expect(qb.updates[0].set).toMatchObject({
      type: 'script',
      scriptLanguage: 'javascript',
    });
    expect(qb.updates[0].unset).toEqual({ config: '' });
  });

  it('refuses to auto-convert an http step so its SSRF guard is not silently dropped', async () => {
    const qb = makeSqlQueryBuilder([
      {
        id: 1,
        key: 'call',
        type: 'http',
        config: { url: 'https://example.com' },
      },
    ]);

    await expect(convertLegacyFlowSteps(qb as any)).rejects.toThrow(
      /cannot be converted automatically/,
    );
    expect(qb.updates).toHaveLength(0);
  });

  it('rejects a legacy step whose table name is not a safe identifier', async () => {
    const qb = makeSqlQueryBuilder([
      {
        id: 1,
        key: 'users',
        type: 'query',
        config: { table: 'enfyra_user; drop' },
      },
    ]);

    await expect(convertLegacyFlowSteps(qb as any)).rejects.toThrow(
      /no valid table name/,
    );
  });

  it('rejects an unknown step type instead of leaving it for the enum narrowing', async () => {
    const qb = makeSqlQueryBuilder([
      { id: 1, key: 'mystery', type: 'smtp', config: {} },
    ]);

    await expect(convertLegacyFlowSteps(qb as any)).rejects.toThrow(
      /unsupported type/,
    );
    expect(qb.updates).toHaveLength(0);
  });

  it('fails loudly when sourceCode is missing but steps still carry config code', async () => {
    const qb = makeSqlQueryBuilder(
      [
        {
          id: 1,
          key: 'legacy_code',
          type: 'script',
          config: { code: 'return 1;' },
        },
      ],
      { hasSourceCode: false },
    );

    await expect(convertLegacyFlowSteps(qb as any)).rejects.toThrow(
      /no sourceCode column/,
    );
  });

  it('is a no-op when the table or collection is absent', async () => {
    const missingTable = makeSqlQueryBuilder([], { hasTable: false });
    await expect(convertLegacyFlowSteps(missingTable as any)).resolves.toBe(0);
    expect(missingTable.updates).toHaveLength(0);

    const missingConfig = makeSqlQueryBuilder(legacyRows, { hasConfig: false });
    await expect(convertLegacyFlowSteps(missingConfig as any)).resolves.toBe(0);
    expect(missingConfig.updates).toHaveLength(0);

    DatabaseConfigService.overrideForTesting('mongodb');
    const missingCollection = makeMongoQueryBuilder([], {
      hasCollection: false,
    });
    await expect(
      convertLegacyFlowSteps(missingCollection as any),
    ).resolves.toBe(0);
    expect(missingCollection.updates).toHaveLength(0);
  });
});
