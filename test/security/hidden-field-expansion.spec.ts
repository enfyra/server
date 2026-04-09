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

function expectNullAs(select: string[], colName: string) {
  const match = select.find((s) => s.includes('NULL') && s.includes(colName));
  expect(match).toBeTruthy();
}

function expectNotRealColumn(select: string[], tableName: string, colName: string) {
  expect(select).not.toContain(`${tableName}.${colName}`);
}

describe('expandFieldsToJoinsAndSelect – hidden field NULL mapping', () => {
  describe('wildcard (*) expansion', () => {
    it('maps hidden columns to NULL in wildcard expansion', async () => {
      const meta = makeMetadata('user_definition', [
        { name: 'id' },
        { name: 'email' },
        { name: 'password', isHidden: true },
        { name: 'name' },
      ]);
      const { select } = await expand('user_definition', ['*'], { user_definition: meta });
      expect(select).toContain('user_definition.id');
      expect(select).toContain('user_definition.email');
      expect(select).toContain('user_definition.name');
      expectNullAs(select, 'password');
      expectNotRealColumn(select, 'user_definition', 'password');
    });

    it('maps multiple hidden columns to NULL', async () => {
      const meta = makeMetadata('post', [
        { name: 'id' },
        { name: 'title' },
        { name: 'body' },
        { name: 'internalNote', isHidden: true },
        { name: 'secret', isHidden: true },
      ]);
      const { select } = await expand('post', ['*'], { post: meta });
      expect(select).toContain('post.title');
      expectNullAs(select, 'internalNote');
      expectNullAs(select, 'secret');
    });

    it('handles table with all columns hidden', async () => {
      const meta = makeMetadata('secrets', [
        { name: 'id', isHidden: true },
        { name: 'token', isHidden: true },
      ]);
      const { select } = await expand('secrets', ['*'], { secrets: meta });
      expectNullAs(select, 'id');
      expectNullAs(select, 'token');
    });
  });

  describe('explicit field requests', () => {
    it('maps an explicit hidden field request to NULL', async () => {
      const meta = makeMetadata('user_definition', [
        { name: 'id' },
        { name: 'email' },
        { name: 'password', isHidden: true },
      ]);
      const { select } = await expand('user_definition', ['id', 'email', 'password'], { user_definition: meta });
      expect(select).toContain('user_definition.id');
      expect(select).toContain('user_definition.email');
      expectNullAs(select, 'password');
      expectNotRealColumn(select, 'user_definition', 'password');
    });

    it('allows non-hidden explicit fields normally', async () => {
      const meta = makeMetadata('post', [
        { name: 'id' },
        { name: 'title' },
        { name: 'secret', isHidden: true },
      ]);
      const { select } = await expand('post', ['id', 'title'], { post: meta });
      expect(select).toContain('post.id');
      expect(select).toContain('post.title');
    });
  });

  describe('isHidden === false is not affected', () => {
    it('includes columns with isHidden explicitly false normally', async () => {
      const meta = makeMetadata('product', [
        { name: 'id' },
        { name: 'price', isHidden: false },
      ]);
      const { select } = await expand('product', ['*'], { product: meta });
      expect(select).toContain('product.price');
    });
  });

  describe('returns empty select for unknown table', () => {
    it('returns empty array when metadata is not found', async () => {
      const { select } = await expand('unknown_table', ['*'], {});
      expect(select).toEqual([]);
    });
  });
});
