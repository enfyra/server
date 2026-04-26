import { expandFieldsToJoinsAndSelect } from 'src/kernel/query';

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

describe('query depth limit – expandFieldsToJoinsAndSelect', () => {
  it('respects maxDepth in field expansion', async () => {
    const { select } = await expandFieldsToJoinsAndSelect(
      'orders',
      ['id', 'total', 'customer.id', 'customer.name'],
      metadataGetter as any,
      'mysql',
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      1,
    );
    expect(select.some((s) => s.includes('orders.id'))).toBe(true);
    expect(select.some((s) => s.includes('customer'))).toBe(true);
  });

  it('no maxDepth allows all nesting', async () => {
    const { select } = await expandFieldsToJoinsAndSelect(
      'orders',
      ['id', 'customer.id', 'customer.role.id'],
      metadataGetter as any,
      'mysql',
    );
    expect(select.some((s) => s.includes('orders.id'))).toBe(true);
    expect(select.some((s) => s.includes('customer'))).toBe(true);
  });
});
