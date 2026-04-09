import { buildNestedSubquery } from '../../src/infrastructure/query-builder/utils/sql/nested-subquery-builder';
import { expandFieldsToJoinsAndSelect } from '../../src/infrastructure/query-builder/utils/sql/expand-fields';

function makeMeta(name: string, columns: any[], relations: any[] = []) {
  return {
    name,
    columns: columns.map((c) => ({ type: 'varchar', ...c })),
    relations,
  };
}

const ordersMeta = makeMeta(
  'orders',
  [{ name: 'id' }, { name: 'total' }],
  [
    {
      propertyName: 'customer',
      type: 'many-to-one',
      targetTableName: 'users',
      foreignKeyColumn: 'customerId',
    },
  ],
);

const usersMeta = makeMeta(
  'users',
  [{ name: 'id' }, { name: 'name' }],
  [
    {
      propertyName: 'role',
      type: 'many-to-one',
      targetTableName: 'roles',
      foreignKeyColumn: 'roleId',
    },
  ],
);

const rolesMeta = makeMeta(
  'roles',
  [{ name: 'id' }, { name: 'title' }],
  [
    {
      propertyName: 'org',
      type: 'many-to-one',
      targetTableName: 'orgs',
      foreignKeyColumn: 'orgId',
    },
  ],
);

const orgsMeta = makeMeta('orgs', [{ name: 'id' }, { name: 'orgName' }]);

const metaMap: Record<string, any> = {
  orders: ordersMeta,
  users: usersMeta,
  roles: rolesMeta,
  orgs: orgsMeta,
};

const metadataGetter = async (t: string) => metaMap[t] ?? null;

describe('query depth limit – buildNestedSubquery', () => {
  it('returns subquery when nestingLevel < maxDepth', async () => {
    const result = await buildNestedSubquery(
      'orders', ordersMeta as any, 'customer', ['id', 'name'],
      'mysql', metadataGetter as any, 0, undefined, undefined, 3,
    );
    expect(result).not.toBeNull();
    expect(result).toContain('users');
  });

  it('returns null when nestingLevel === maxDepth', async () => {
    const result = await buildNestedSubquery(
      'orders', ordersMeta as any, 'customer', ['id'],
      'mysql', metadataGetter as any, 2, undefined, undefined, 2,
    );
    expect(result).toBeNull();
  });

  it('returns null when nestingLevel > maxDepth', async () => {
    const result = await buildNestedSubquery(
      'orders', ordersMeta as any, 'customer', ['id'],
      'mysql', metadataGetter as any, 5, undefined, undefined, 3,
    );
    expect(result).toBeNull();
  });

  it('allows unlimited depth when maxDepth is undefined', async () => {
    const result = await buildNestedSubquery(
      'orders', ordersMeta as any, 'customer', ['id', 'name'],
      'mysql', metadataGetter as any, 100, undefined, undefined, undefined,
    );
    expect(result).not.toBeNull();
  });

  it('maxDepth=1 allows first level but blocks nested relations', async () => {
    const result = await buildNestedSubquery(
      'orders', ordersMeta as any, 'customer', ['id', 'name', 'role.id'],
      'mysql', metadataGetter as any, 0, undefined, undefined, 1,
    );
    expect(result).not.toBeNull();
    expect(result).toContain('users');
    expect(result).not.toContain('roles');
  });

  it('maxDepth=2 allows two levels of nesting', async () => {
    const result = await buildNestedSubquery(
      'orders', ordersMeta as any, 'customer', ['id', 'role.id', 'role.title'],
      'mysql', metadataGetter as any, 0, undefined, undefined, 2,
    );
    expect(result).not.toBeNull();
    expect(result).toContain('users');
    expect(result).toContain('roles');
  });

  it('maxDepth=2 blocks third level', async () => {
    const result = await buildNestedSubquery(
      'orders', ordersMeta as any, 'customer', ['id', 'role.org.orgName'],
      'mysql', metadataGetter as any, 0, undefined, undefined, 2,
    );
    expect(result).not.toBeNull();
    expect(result).toContain('users');
    expect(result).not.toContain('orgs');
  });
});

describe('query depth limit – expandFieldsToJoinsAndSelect', () => {
  it('respects maxDepth in field expansion', async () => {
    const { select } = await expandFieldsToJoinsAndSelect(
      'orders', ['id', 'total', 'customer.id', 'customer.name'],
      metadataGetter as any, 'mysql',
      undefined, undefined, undefined, undefined, undefined, undefined, 1,
    );
    expect(select.some((s) => s.includes('orders.id'))).toBe(true);
    expect(select.some((s) => s.includes('customer'))).toBe(true);
  });

  it('no maxDepth allows all nesting', async () => {
    const { select } = await expandFieldsToJoinsAndSelect(
      'orders', ['id', 'customer.id', 'customer.role.id'],
      metadataGetter as any, 'mysql',
    );
    expect(select.some((s) => s.includes('orders.id'))).toBe(true);
    expect(select.some((s) => s.includes('customer'))).toBe(true);
  });
});
