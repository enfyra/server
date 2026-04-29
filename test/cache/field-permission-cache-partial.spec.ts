import { describe, it, expect, vi } from 'vitest';
import { EventEmitter2 } from 'eventemitter2';
import { FieldPermissionCacheService } from '../../src/engines/cache';

function makeRow(overrides: any) {
  return {
    id: 1,
    isEnabled: true,
    action: 'read',
    effect: 'allow',
    role: { id: 10 },
    allowedUsers: [],
    column: { id: 1, name: 'name', table: { id: 1, name: 'post' } },
    relation: null,
    condition: null,
    ...overrides,
  };
}

function makeQb(rows: any[]) {
  return {
    find: vi.fn(async (args: any) => {
      const idsFilter = args?.filter?.id?._in;
      if (idsFilter) {
        const set = new Set(idsFilter.map(String));
        return { data: rows.filter((r) => set.has(String(r.id))) };
      }
      return { data: rows };
    }),
  } as any;
}

function makeMetadata(rows: any[]) {
  const tables = new Map<string, any>();
  for (const row of rows) {
    const column = row.column;
    const columnTableName = column?.table?.name;
    if (column?.id && column?.name && columnTableName) {
      const table = tables.get(columnTableName) ?? {
        id: column.table.id,
        name: columnTableName,
        columns: [],
        relations: [],
      };
      if (!table.columns.some((c: any) => String(c.id) === String(column.id))) {
        table.columns.push({ id: column.id, name: column.name });
      }
      tables.set(columnTableName, table);
    }

    const relation = row.relation;
    const sourceTableName = relation?.sourceTable?.name;
    if (relation?.id && relation?.propertyName && sourceTableName) {
      const table = tables.get(sourceTableName) ?? {
        id: relation.sourceTable.id,
        name: sourceTableName,
        columns: [],
        relations: [],
      };
      if (
        !table.relations.some((r: any) => String(r.id) === String(relation.id))
      ) {
        table.relations.push({
          id: relation.id,
          propertyName: relation.propertyName,
        });
      }
      tables.set(sourceTableName, table);
    }
  }

  return {
    tables,
    tablesList: [...tables.values()],
    version: 1,
    timestamp: new Date(),
  };
}

function makeService(rows: any[], metadata = makeMetadata(rows)) {
  const qb = makeQb(rows);
  const svc = new FieldPermissionCacheService({
    queryBuilderService: qb,
    metadataCacheService: {
      getDirectMetadata: vi.fn(() => metadata),
    } as any,
    eventEmitter: new EventEmitter2(),
  });
  return { svc, qb };
}

describe('FieldPermissionCacheService — partial reload', () => {
  it('supportsPartialReload returns true', () => {
    const { svc } = makeService([]);
    expect(svc.supportsPartialReload()).toBe(true);
  });

  it('partialReload inserts new rule, indexes column into allow Set', async () => {
    const data: any[] = [
      makeRow({
        id: 1,
        column: { id: 1, name: 'name', table: { id: 1, name: 'post' } },
      }),
    ];
    const { svc } = makeService(data);
    await svc.reload(false);

    data.push(
      makeRow({
        id: 2,
        column: { id: 2, name: 'email', table: { id: 1, name: 'post' } },
      }),
    );
    await svc.partialReload(
      {
        table: 'field_permission_definition',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [2],
      },
      false,
    );

    const policies = await svc.getPoliciesFor(
      { id: 99, role: { id: 10 } },
      'post',
      'read',
    );
    expect(policies).toHaveLength(1);
    expect(policies[0].rules.map((r) => r.id).sort()).toEqual([1, 2]);
    expect(policies[0].unconditionalAllowedColumns.has('name')).toBe(true);
    expect(policies[0].unconditionalAllowedColumns.has('email')).toBe(true);
  });

  it('loads lean field permission rows and resolves table names from metadata cache', async () => {
    const metadata = {
      tables: new Map(),
      tablesList: [
        {
          id: 1,
          name: 'post',
          columns: [{ id: 101, name: 'title' }],
          relations: [],
        },
      ],
      version: 1,
      timestamp: new Date(),
    };
    const data: any[] = [
      makeRow({
        id: 1,
        column: { id: 101 },
      }),
    ];
    const { svc, qb } = makeService(data, metadata);
    await svc.reload(false);

    expect(qb.find).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'field_permission_definition',
        fields: [
          'id',
          'isEnabled',
          'action',
          'effect',
          'condition',
          'role.id',
          'allowedUsers.id',
          'column.id',
          'relation.id',
        ],
      }),
    );

    const policies = await svc.getPoliciesFor(
      { id: 99, role: { id: 10 } },
      'post',
      'read',
    );
    expect(policies).toHaveLength(1);
    expect(policies[0].rules[0].tableName).toBe('post');
    expect(policies[0].rules[0].columnName).toBe('title');
  });

  it('resolves relation rules from metadata cache without fetching relation sourceTable', async () => {
    const metadata = {
      tables: new Map(),
      tablesList: [
        {
          id: 1,
          name: 'post',
          columns: [],
          relations: [{ id: 201, propertyName: 'author' }],
        },
      ],
      version: 1,
      timestamp: new Date(),
    };
    const data: any[] = [
      makeRow({
        id: 1,
        column: null,
        relation: { id: 201 },
      }),
    ];
    const { svc } = makeService(data, metadata);
    await svc.reload(false);

    const policies = await svc.getPoliciesFor(
      { id: 99, role: { id: 10 } },
      'post',
      'read',
    );
    expect(policies).toHaveLength(1);
    expect(policies[0].rules[0].tableName).toBe('post');
    expect(policies[0].rules[0].relationPropertyName).toBe('author');
  });

  it('partialReload removes deleted rule and rebuilds bucket Sets', async () => {
    const data: any[] = [
      makeRow({
        id: 1,
        column: { id: 1, name: 'name', table: { id: 1, name: 'post' } },
      }),
      makeRow({
        id: 2,
        column: { id: 2, name: 'email', table: { id: 1, name: 'post' } },
      }),
    ];
    const { svc } = makeService(data);
    await svc.reload(false);

    data.splice(1, 1);
    await svc.partialReload(
      {
        table: 'field_permission_definition',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [2],
      },
      false,
    );

    const policies = await svc.getPoliciesFor(
      { id: 99, role: { id: 10 } },
      'post',
      'read',
    );
    expect(policies).toHaveLength(1);
    expect(policies[0].rules).toHaveLength(1);
    expect(policies[0].unconditionalAllowedColumns.has('name')).toBe(true);
    expect(policies[0].unconditionalAllowedColumns.has('email')).toBe(false);
  });

  it('partialReload removes bucket entirely when last rule is deleted', async () => {
    const data: any[] = [
      makeRow({
        id: 1,
        column: { id: 1, name: 'name', table: { id: 1, name: 'post' } },
      }),
    ];
    const { svc } = makeService(data);
    await svc.reload(false);

    data.length = 0;
    await svc.partialReload(
      {
        table: 'field_permission_definition',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [1],
      },
      false,
    );

    const policies = await svc.getPoliciesFor(
      { id: 99, role: { id: 10 } },
      'post',
      'read',
    );
    expect(policies).toHaveLength(0);
  });

  it('partialReload moves rule across buckets when role/action changes', async () => {
    const data: any[] = [
      makeRow({
        id: 1,
        role: { id: 10 },
        action: 'read',
        column: { id: 1, name: 'name', table: { id: 1, name: 'post' } },
      }),
    ];
    const { svc } = makeService(data);
    await svc.reload(false);

    data[0] = makeRow({
      id: 1,
      role: { id: 20 },
      action: 'update',
      column: { id: 1, name: 'name', table: { id: 1, name: 'post' } },
    });

    await svc.partialReload(
      {
        table: 'field_permission_definition',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [1],
      },
      false,
    );

    const oldBucket = await svc.getPoliciesFor(
      { id: 99, role: { id: 10 } },
      'post',
      'read',
    );
    expect(oldBucket).toHaveLength(0);

    const newBucket = await svc.getPoliciesFor(
      { id: 99, role: { id: 20 } },
      'post',
      'update',
    );
    expect(newBucket).toHaveLength(1);
    expect(newBucket[0].rules[0].id).toBe(1);
    expect(newBucket[0].unconditionalAllowedColumns.has('name')).toBe(true);
  });

  it('indexes allowedUsers into direct per-user buckets', async () => {
    const data: any[] = [
      makeRow({
        id: 1,
        role: null,
        allowedUsers: [{ id: 100 }, { id: 200 }],
        column: { id: 1, name: 'name', table: { id: 1, name: 'post' } },
      }),
    ];
    const { svc } = makeService(data);
    await svc.reload(false);

    const cache = svc.getRawCache();
    expect(cache.has('u:100|post|read')).toBe(true);
    expect(cache.has('u:200|post|read')).toBe(true);
    expect(cache.has('u:100,200|post|read')).toBe(false);

    const matchingPolicies = await svc.getPoliciesFor(
      { id: 100, role: { id: 10 } },
      'post',
      'read',
    );
    expect(matchingPolicies).toHaveLength(1);
    expect(matchingPolicies[0].rules[0].id).toBe(1);

    const otherPolicies = await svc.getPoliciesFor(
      { id: 300, role: { id: 10 } },
      'post',
      'read',
    );
    expect(otherPolicies).toHaveLength(0);
  });

  it('partialReload treats isEnabled=false as effective delete', async () => {
    const data: any[] = [
      makeRow({
        id: 1,
        column: { id: 1, name: 'name', table: { id: 1, name: 'post' } },
      }),
    ];
    const { svc, qb } = makeService(data);
    await svc.reload(false);

    qb.find.mockImplementationOnce(async (args: any) => {
      if (args?.filter?.id?._in) return { data: [] };
      return { data };
    });

    await svc.partialReload(
      {
        table: 'field_permission_definition',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [1],
      },
      false,
    );

    const policies = await svc.getPoliciesFor(
      { id: 99, role: { id: 10 } },
      'post',
      'read',
    );
    expect(policies).toHaveLength(0);
  });

  it('deny rule indexed into deny Set, removing it clears deny entry', async () => {
    const data: any[] = [
      makeRow({
        id: 1,
        effect: 'deny',
        column: {
          id: 1,
          name: 'secret',
          table: { id: 1, name: 'user_definition' },
        },
      }),
    ];
    const { svc } = makeService(data);
    await svc.reload(false);

    let policies = await svc.getPoliciesFor(
      { id: 99, role: { id: 10 } },
      'user_definition',
      'read',
    );
    expect(policies[0].unconditionalDeniedColumns.has('secret')).toBe(true);

    data.length = 0;
    await svc.partialReload(
      {
        table: 'field_permission_definition',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [1],
      },
      false,
    );

    policies = await svc.getPoliciesFor(
      { id: 99, role: { id: 10 } },
      'user_definition',
      'read',
    );
    expect(policies).toHaveLength(0);
  });

  it('conditional rule (with condition) does NOT add to unconditional Sets', async () => {
    const data: any[] = [
      makeRow({
        id: 1,
        condition: { _and: [{ status: { _eq: 'active' } }] },
        column: { id: 1, name: 'name', table: { id: 1, name: 'post' } },
      }),
    ];
    const { svc } = makeService(data);
    await svc.reload(false);

    data.push(
      makeRow({
        id: 2,
        column: { id: 2, name: 'email', table: { id: 1, name: 'post' } },
      }),
    );
    await svc.partialReload(
      {
        table: 'field_permission_definition',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [2],
      },
      false,
    );

    const policies = await svc.getPoliciesFor(
      { id: 99, role: { id: 10 } },
      'post',
      'read',
    );
    expect(policies[0].rules).toHaveLength(2);
    expect(policies[0].unconditionalAllowedColumns.has('name')).toBe(false);
    expect(policies[0].unconditionalAllowedColumns.has('email')).toBe(true);
  });

  it('partialReload with empty ids is a no-op', async () => {
    const data: any[] = [
      makeRow({
        id: 1,
        column: { id: 1, name: 'name', table: { id: 1, name: 'post' } },
      }),
    ];
    const { svc, qb } = makeService(data);
    await svc.reload(false);
    qb.find.mockClear();

    await svc.partialReload(
      {
        table: 'field_permission_definition',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [],
      },
      false,
    );

    expect(qb.find).not.toHaveBeenCalled();
  });
});
