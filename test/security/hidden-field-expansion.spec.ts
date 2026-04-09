import { expandFieldsToJoinsAndSelect } from '../../src/infrastructure/query-builder/utils/sql/expand-fields';

function makeMetadata(
  name: string,
  columns: Array<{ name: string; type?: string; isHidden?: boolean }>,
  relations: any[] = [],
) {
  return {
    name,
    columns: columns.map((c) => ({ type: 'varchar', ...c })),
    relations,
  };
}

async function expand(
  tableName: string,
  fields: string[],
  metaMap: Record<string, any>,
) {
  const getter = async (t: string) => metaMap[t] ?? null;
  return expandFieldsToJoinsAndSelect(tableName, fields, getter, 'mysql');
}

describe('expandFieldsToJoinsAndSelect – hidden fields are not mapped to NULL', () => {
  it('wildcard (*) includes hidden columns in select', async () => {
    const meta = makeMetadata('user_definition', [
      { name: 'id' },
      { name: 'email' },
      { name: 'password', isHidden: true },
      { name: 'name' },
    ]);
    const { select } = await expand('user_definition', ['*'], {
      user_definition: meta,
    });
    expect(select).toContain('user_definition.id');
    expect(select).toContain('user_definition.email');
    expect(select).toContain('user_definition.name');
    expect(select).toContain('user_definition.password');
  });

  it('explicit field list includes hidden columns in select', async () => {
    const meta = makeMetadata('user_definition', [
      { name: 'id' },
      { name: 'email' },
      { name: 'password', isHidden: true },
    ]);
    const { select } = await expand(
      'user_definition',
      ['id', 'email', 'password'],
      { user_definition: meta },
    );
    expect(select).toContain('user_definition.id');
    expect(select).toContain('user_definition.email');
    expect(select).toContain('user_definition.password');
  });

  it('unknown table returns empty select', async () => {
    const { select } = await expand('unknown_table', ['*'], {});
    expect(select).toEqual([]);
  });
});
