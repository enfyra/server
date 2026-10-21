import { describe, expect, it, vi } from 'vitest';
import {
  CodeAuditService,
  projectCodeAuditPreImage,
} from '../../src/modules/dynamic-api/services/code-audit.service';
import { SYSTEM_TABLES } from '../../src/shared/utils/system-tables.constants';

const tableMetadata = {
  columns: [
    { name: 'id' },
    { name: 'name' },
    { name: 'version' },
    { name: 'sourceCode' },
    { name: 'compiledCode' },
    { name: 'createdAt' },
    { name: 'updatedAt' },
  ],
};

describe('projectCodeAuditPreImage', () => {
  it('keeps only declared columns minus identity and derived fields', () => {
    const previous = projectCodeAuditPreImage(
      {
        id: 7,
        name: 'handler',
        version: '1.0.0',
        sourceCode: 'return 1;',
        compiledCode: 'compiled()',
        createdAt: new Date('2026-01-01'),
        updatedAt: new Date('2026-01-02'),
        route: { id: 3 },
      },
      tableMetadata,
    );

    expect(previous).toEqual({
      name: 'handler',
      version: '1.0.0',
      sourceCode: 'return 1;',
    });
  });

  it('returns null when the record or its metadata is missing', () => {
    expect(projectCodeAuditPreImage(null, tableMetadata)).toBeNull();
    expect(projectCodeAuditPreImage({ id: 1 }, null)).toBeNull();
    expect(projectCodeAuditPreImage({ id: 1 }, { columns: [] })).toBeNull();
  });

  it('keeps an unpublished column out of the append-only log', () => {
    const previous = projectCodeAuditPreImage(
      {
        provider: 'google',
        clientSecret: 'super-secret',
        version: '1.0.0',
        sourceCode: 'return 1;',
      },
      {
        columns: [
          { name: 'provider' },
          { name: 'clientSecret', isPublished: false },
          { name: 'version' },
          { name: 'sourceCode' },
        ],
      },
    );

    expect(previous).toEqual({
      provider: 'google',
      version: '1.0.0',
      sourceCode: 'return 1;',
    });
  });
});

function createSqlQueryBuilder() {
  const inserted: any[] = [];
  const knex = vi.fn(() => ({
    insert: vi.fn(async (row: Record<string, any>) => {
      inserted.push(row);
    }),
  })) as any;

  return {
    inserted,
    queryBuilderService: {
      isMongoDb: () => false,
      getKnex: () => knex,
    } as any,
  };
}

describe('CodeAuditService', () => {
  it('appends the pre-image and caller provenance to the audit table', async () => {
    const { inserted, queryBuilderService } = createSqlQueryBuilder();
    const service = new CodeAuditService({ queryBuilderService });

    await service.record({
      tableName: SYSTEM_TABLES.routeHandler,
      recordId: 12,
      action: 'update',
      fromVersion: '1.0.0',
      toVersion: '1.0.1',
      previous: { name: 'handler', sourceCode: 'return 1;' },
      caller: { id: 4, email: 'root@example.com', source: 'pat' },
    });

    expect(inserted).toHaveLength(1);
    const row = inserted[0];
    expect(row.tableName).toBe('enfyra_route_handler');
    expect(row.recordId).toBe('12');
    expect(row.action).toBe('update');
    expect(row.fromVersion).toBe('1.0.0');
    expect(row.toVersion).toBe('1.0.1');
    expect(row.changedById).toBe(4);
    expect(row.changedBySource).toBe('pat');
    expect(row.changedByEmail).toBe('root@example.com');
    expect(JSON.parse(row.previous)).toEqual({
      name: 'handler',
      sourceCode: 'return 1;',
    });
  });

  it('records a delete entry without a toVersion or restored version', async () => {
    const { inserted, queryBuilderService } = createSqlQueryBuilder();
    const service = new CodeAuditService({ queryBuilderService });

    await service.record({
      tableName: SYSTEM_TABLES.helper,
      recordId: 'abc',
      action: 'delete',
      fromVersion: '2.0.0',
      previous: { name: 'helper' },
    });

    const row = inserted[0];
    expect(row.recordId).toBe('abc');
    expect(row.action).toBe('delete');
    expect(row.toVersion).toBeNull();
    expect(row.restoredFrom).toBeNull();
    expect(row.changedById).toBeNull();
  });

  it('stores the caller as a relation value on MongoDB', async () => {
    const insertOne = vi.fn().mockResolvedValue({ acknowledged: true });
    const queryBuilderService = {
      isMongoDb: () => true,
      getMongoDb: () => ({ collection: () => ({ insertOne }) }),
    } as any;
    const service = new CodeAuditService({ queryBuilderService });

    await service.record({
      tableName: SYSTEM_TABLES.websocket,
      recordId: '65f000000000000000000001',
      action: 'revert',
      fromVersion: '3.0.0',
      toVersion: '3.0.1',
      restoredFrom: '2.0.0',
      previous: { name: 'socket' },
      caller: { objectId: '65f000000000000000000009', source: 'jwt' },
    });

    const row = insertOne.mock.calls[0][0];
    expect(row.restoredFrom).toBe('2.0.0');
    expect(row.previous).toEqual({ name: 'socket' });
    expect(String(row.changedBy)).toBe('65f000000000000000000009');
  });

  it('surfaces a failed audit write instead of reporting complete history', async () => {
    const queryBuilderService = {
      isMongoDb: () => false,
      getKnex: () =>
        vi.fn(() => ({
          insert: vi.fn(async () => {
            throw new Error('audit table missing');
          }),
        })),
    } as any;
    const service = new CodeAuditService({ queryBuilderService });

    await expect(
      service.record({
        tableName: SYSTEM_TABLES.preHook,
        recordId: 1,
        action: 'create',
        toVersion: '1.0.0',
      }),
    ).rejects.toThrow('audit table missing');
  });
});
