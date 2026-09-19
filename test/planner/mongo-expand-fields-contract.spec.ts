import { expandFieldsMongo } from '@enfyra/kernel';

const metadata = {
  tables: new Map([
    ['posts', {
      columns: [{ name: '_id' }],
      relations: [{
        propertyName: 'author',
        targetTableName: 'users',
        type: 'many-to-one',
        foreignKeyColumn: 'authorId',
      }],
    }],
  ]),
};

describe('Mongo field expansion contracts', () => {
  it('preserves a declared dotted scalar column', async () => {
    const expanded = await expandFieldsMongo({ tables: new Map([['items', { columns: [{ name: 'address.city' }], relations: [] }]]) }, 'items', ['address.city']);
    expect(expanded).toEqual({ scalarFields: ['address.city'], relations: [] });
  });
  it('makes bare and nested relation selections additive regardless of order', async () => {
    const bareFirst = await expandFieldsMongo(
      metadata,
      'posts',
      ['author', 'author.name'],
    );
    const nestedFirst = await expandFieldsMongo(
      metadata,
      'posts',
      ['author.name', 'author'],
    );

    expect(bareFirst.relations[0].nestedFields).toEqual(['_id', 'name']);
    expect(nestedFirst.relations[0].nestedFields).toEqual(['name', '_id']);
  });
});
