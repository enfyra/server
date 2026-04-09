import { PolicyService } from '../../src/core/policy/policy.service';

describe('PolicyService.checkSchemaMigration', () => {
  const policy = new PolicyService({} as any, {} as any);

  it('allows create without hash', async () => {
    const d = await policy.checkSchemaMigration({
      operation: 'create',
      tableName: 'items',
      beforeMetadata: null,
      afterMetadata: null,
      requestContext: {} as any,
    });
    expect(d.allow).toBe(true);
    expect((d.details as any).schemaChanged).toBe(true);
    expect((d.details as any).isDestructive).toBe(false);
  });

  it('allows delete and marks destructive', async () => {
    const d = await policy.checkSchemaMigration({
      operation: 'delete',
      tableName: 'items',
      beforeMetadata: null,
      afterMetadata: null,
      requestContext: {} as any,
    });
    expect(d.allow).toBe(true);
    expect((d.details as any).isDestructive).toBe(true);
  });

  it('update with missing before or after allows with reason', async () => {
    const d = await policy.checkSchemaMigration({
      operation: 'update',
      tableName: 'items',
      beforeMetadata: null,
      afterMetadata: { columns: [] },
      requestContext: {} as any,
    });
    expect(d.allow).toBe(true);
    expect((d.details as any).reason).toBe('missing_before_after');
  });

  it('update with identical normalized metadata reports no schema change', async () => {
    const meta = {
      name: 'items',
      columns: [
        { id: 'b', name: 'b', type: 'string' },
        { id: 'a', name: 'a', type: 'string' },
      ],
      relations: [],
    };
    const d = await policy.checkSchemaMigration({
      operation: 'update',
      tableName: 'items',
      beforeMetadata: meta,
      afterMetadata: {
        name: 'items',
        columns: [
          { id: 'a', name: 'a', type: 'string' },
          { id: 'b', name: 'b', type: 'string' },
        ],
        relations: [],
      },
      requestContext: {} as any,
    });
    expect(d.allow).toBe(true);
    expect((d.details as any).schemaChanged).toBe(false);
  });

  it('update with removed column returns preview and required hash', async () => {
    const before = {
      name: 'items',
      columns: [{ id: '1', name: 'title', type: 'string' }],
      relations: [],
    };
    const after = { name: 'items', columns: [], relations: [] };
    const d = await policy.checkSchemaMigration({
      operation: 'update',
      tableName: 'items',
      beforeMetadata: before,
      afterMetadata: after,
      requestContext: { $query: {} } as any,
    });
    expect(d.allow).toBe(false);
    expect((d as any).preview).toBe(true);
    expect((d.details as any).isDestructive).toBe(true);
    expect((d.details as any).removedColumns).toEqual(['title']);
    const hash = (d.details as any).requiredConfirmHash as string;
    expect(hash.length).toBe(64);

    const ok = await policy.checkSchemaMigration({
      operation: 'update',
      tableName: 'items',
      beforeMetadata: before,
      afterMetadata: after,
      requestContext: { $query: { schemaConfirmHash: hash } } as any,
    });
    expect(ok.allow).toBe(true);
    expect((ok.details as any).requiredConfirmHash).toBe(hash);
  });

  it('accepts schema_confirm_hash query alias', async () => {
    const before = {
      name: 'items',
      columns: [{ id: '1', name: 'title', type: 'string' }],
      relations: [],
    };
    const after = { name: 'items', columns: [], relations: [] };
    const preview = await policy.checkSchemaMigration({
      operation: 'update',
      tableName: 'items',
      beforeMetadata: before,
      afterMetadata: after,
      requestContext: { $query: {} } as any,
    });
    const hash = (preview.details as any).requiredConfirmHash as string;
    const ok = await policy.checkSchemaMigration({
      operation: 'update',
      tableName: 'items',
      beforeMetadata: before,
      afterMetadata: after,
      requestContext: { $query: { schema_confirm_hash: hash } } as any,
    });
    expect(ok.allow).toBe(true);
  });

  it('rejects wrong confirm hash with 422', async () => {
    const before = {
      name: 'items',
      columns: [{ id: '1', name: 'title', type: 'string' }],
      relations: [],
    };
    const after = { name: 'items', columns: [], relations: [] };
    const d = await policy.checkSchemaMigration({
      operation: 'update',
      tableName: 'items',
      beforeMetadata: before,
      afterMetadata: after,
      requestContext: { $query: { schemaConfirmHash: '0'.repeat(64) } } as any,
    });
    expect(d.allow).toBe(false);
    expect(d.statusCode).toBe(422);
    expect(d.code).toBe('SCHEMA_CONFIRM_HASH_MISMATCH');
  });

  it('parses uniques string JSON for diff', async () => {
    const before = {
      name: 'items',
      columns: [{ id: '1', name: 'title', type: 'string' }],
      relations: [],
      uniques: JSON.stringify([['title']]),
    };
    const after = {
      name: 'items',
      columns: [{ id: '1', name: 'title', type: 'string' }],
      relations: [],
      uniques: JSON.stringify([]),
    };
    const d = await policy.checkSchemaMigration({
      operation: 'update',
      tableName: 'items',
      beforeMetadata: before,
      afterMetadata: after,
      requestContext: { $query: {} } as any,
    });
    expect(d.allow).toBe(false);
    expect((d as any).preview).toBe(true);
    expect((d.details as any).removedUniques.length).toBeGreaterThan(0);
  });
});
