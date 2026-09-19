import { parseFields, JoinRegistry } from '@enfyra/kernel';

const META = {
  tables: new Map<string, any>([
    [
      'posts',
      {
        name: 'posts',
        columns: [
          { name: 'id', type: 'integer' },
          { name: 'title', type: 'varchar' },
          { name: 'status', type: 'varchar' },
        ],
        relations: [
          {
            propertyName: 'author',
            type: 'many-to-one',
            targetTableName: 'users',
            isInverse: false,
          },
          {
            propertyName: 'tags',
            type: 'many-to-many',
            targetTableName: 'tags',
            isInverse: false,
          },
        ],
      },
    ],
    [
      'users',
      {
        name: 'users',
        columns: [
          { name: 'id', type: 'integer' },
          { name: 'name', type: 'varchar' },
          { name: 'email', type: 'varchar' },
        ],
        relations: [
          {
            propertyName: 'posts',
            type: 'one-to-many',
            targetTableName: 'posts',
            isInverse: true,
            mappedBy: 'author',
          },
        ],
      },
    ],
    [
      'tags',
      {
        name: 'tags',
        columns: [
          { name: 'id', type: 'integer' },
          { name: 'label', type: 'varchar' },
        ],
        relations: [],
      },
    ],
  ]),
};

function parse(fields: string[], table = 'posts') {
  const registry = new JoinRegistry();
  const tree = parseFields(fields, table, META, registry);
  return { tree, joinCount: registry.getAll().length };
}

describe('field-parser', () => {
  it('rejects non-array and non-string field entries', () => {
    expect(() =>
      parseFields('id' as any, 'posts', META, new JoinRegistry()),
    ).toThrow(/fields.*array/i);
    expect(() =>
      parseFields(['id', 7] as any, 'posts', META, new JoinRegistry()),
    ).toThrow(/field entries.*strings/i);
  });

  it('parses scalar fields', () => {
    const { tree } = parse(['id', 'title']);
    expect(tree.nodes).toEqual([
      { kind: 'scalar', name: 'id' },
      { kind: 'scalar', name: 'title' },
    ]);
  });

  it('parses wildcard *', () => {
    const { tree } = parse(['*']);
    expect(tree.nodes[0]).toEqual({ kind: 'wildcard' });
    expect(tree.nodes.find((n) => n.kind === 'relation')).toBeDefined();
  });

  it('expands * with all relations as id-only', () => {
    const { tree } = parse(['*']);
    const author = tree.nodes.find(
      (n) => n.kind === 'relation' && n.propertyName === 'author',
    );
    expect(author).toBeDefined();
    expect((author as any).children).toEqual([{ kind: 'scalar', name: 'id' }]);
  });

  it('parses dotted path as nested relation', () => {
    const { tree } = parse(['author.name']);
    const author = tree.nodes.find(
      (n) => n.kind === 'relation' && n.propertyName === 'author',
    );
    expect(author).toBeDefined();
    expect((author as any).children).toEqual([
      { kind: 'scalar', name: 'name' },
    ]);
  });

  it('parses bare relation name as id-only', () => {
    const { tree } = parse(['author']);
    const author = tree.nodes.find(
      (n) => n.kind === 'relation' && n.propertyName === 'author',
    );
    expect((author as any).children).toEqual([{ kind: 'scalar', name: 'id' }]);
  });

  it('registers join for M2O relations', () => {
    const { joinCount } = parse(['author.name']);
    expect(joinCount).toBe(1);
  });

  it('does NOT register join for M2M relations (batch-fetch)', () => {
    const { joinCount } = parse(['tags.label']);
    expect(joinCount).toBe(0);
  });

  it('does NOT register join for O2M relations', () => {
    const { joinCount } = parse(['posts.title'], 'users');
    expect(joinCount).toBe(0);
  });

  it('handles nested 2-level paths', () => {
    const { tree } = parse(['posts.author.name'], 'users');
    const posts = tree.nodes.find(
      (n) => n.kind === 'relation' && n.propertyName === 'posts',
    );
    const nestedAuthor = (posts as any).children.find(
      (c: any) => c.kind === 'relation' && c.propertyName === 'author',
    );
    expect(nestedAuthor).toBeDefined();
    expect(nestedAuthor.children).toEqual([{ kind: 'scalar', name: 'name' }]);
  });

  it('mixes wildcard and explicit relation expansion', () => {
    const { tree } = parse(['*', 'author.name']);
    expect(tree.nodes[0]).toEqual({ kind: 'wildcard' });
    const author = tree.nodes.find(
      (n) => n.kind === 'relation' && n.propertyName === 'author',
    );
    expect((author as any).children).toEqual([
      { kind: 'scalar', name: 'name' },
    ]);
  });

  it('returns empty tree for empty input', () => {
    const { tree } = parse([]);
    expect(tree.nodes).toEqual([]);
  });

  it('merges bare and nested relation fields regardless of order', () => {
    const first = parse(['author', 'author.name']).tree;
    const second = parse(['author.name', 'author']).tree;
    const firstAuthor = first.nodes.find(
      (node) => node.kind === 'relation' && node.propertyName === 'author',
    );
    const secondAuthor = second.nodes.find(
      (node) => node.kind === 'relation' && node.propertyName === 'author',
    );
    expect((firstAuthor as any).children).toEqual(
      expect.arrayContaining([
        { kind: 'scalar', name: 'id' },
        { kind: 'scalar', name: 'name' },
      ]),
    );
    expect((secondAuthor as any).children).toEqual(
      expect.arrayContaining([
        { kind: 'scalar', name: 'id' },
        { kind: 'scalar', name: 'name' },
      ]),
    );
  });

  it('gives a real id column precedence over the primary-key alias', () => {
    const aliasedMetadata = {
      tables: new Map([
        [
          'rows',
          {
            name: 'rows',
            columns: [
              { name: '_id', type: 'objectid', isPrimary: true },
              { name: 'id', type: 'varchar', isPrimary: false },
            ],
            relations: [],
          },
        ],
      ]),
    };
    const tree = parseFields(
      ['id'],
      'rows',
      aliasedMetadata,
      new JoinRegistry(),
    );
    expect(tree.nodes).toEqual([{ kind: 'scalar', name: 'id' }]);
  });

  it('deduplicates repeated scalar and nested field selections', () => {
    const tree = parse(['title', 'title', 'author.name', 'author.name']).tree;
    expect(tree.nodes.filter((node) => node.kind === 'scalar')).toEqual([
      { kind: 'scalar', name: 'title' },
    ]);
    const author = tree.nodes.find(
      (node) => node.kind === 'relation' && node.propertyName === 'author',
    );
    expect((author as any).children).toEqual([
      { kind: 'scalar', name: 'name' },
    ]);
  });

  it('uses the target table primary key for bare relation expansion', () => {
    const mongoMetadata = {
      tables: new Map([
        [
          'posts',
          {
            name: 'posts',
            columns: [{ name: 'id', type: 'integer' }],
            relations: [
              {
                propertyName: 'author',
                type: 'many-to-one',
                targetTableName: 'authors',
              },
            ],
          },
        ],
        [
          'authors',
          {
            name: 'authors',
            columns: [{ name: '_id', type: 'objectid', isPrimary: true }],
            relations: [],
          },
        ],
      ]),
    };
    const tree = parseFields(
      ['author'],
      'posts',
      mongoMetadata,
      new JoinRegistry(),
    );
    const author = tree.nodes.find(
      (node) => node.kind === 'relation' && node.propertyName === 'author',
    );
    expect((author as any).children).toEqual([
      { kind: 'scalar', name: '_id' },
    ]);
  });

  it('restores the join registry when parsing fails', () => {
    const registry = new JoinRegistry();
    const cyclic = {
      tables: new Map([
        ...META.tables.entries(),
        [
          'nodes',
          {
            name: 'nodes',
            columns: [{ name: 'id', type: 'integer', isPrimary: true }],
            relations: [
              {
                propertyName: 'child',
                type: 'many-to-one',
                targetTableName: 'nodes',
              },
            ],
          },
        ],
      ]),
    };
    const path = `${Array.from({ length: 70 }, () => 'child').join('.')}.id`;
    expect(() =>
      parseFields(['child.id', path], 'nodes', cyclic, registry),
    ).toThrow(/field nesting depth/i);
    expect(registry.getAll()).toEqual([]);
  });

  it('expands bare relations to every composite primary-key component', () => {
    const compositeMetadata = {
      tables: new Map([
        [
          'parents',
          {
            name: 'parents',
            columns: [{ name: 'id', type: 'integer', isPrimary: true }],
            relations: [
              {
                propertyName: 'child',
                type: 'many-to-one',
                targetTableName: 'children',
              },
            ],
          },
        ],
        [
          'children',
          {
            name: 'children',
            columns: [
              { name: 'tenantId', type: 'integer', isPrimary: true },
              { name: 'sequence', type: 'integer', isPrimary: true },
            ],
            relations: [],
          },
        ],
      ]),
    };
    const tree = parseFields(
      ['child'],
      'parents',
      compositeMetadata,
      new JoinRegistry(),
    );
    const child = tree.nodes.find(
      (node) => node.kind === 'relation' && node.propertyName === 'child',
    );
    expect((child as any).children).toEqual([
      { kind: 'scalar', name: 'tenantId' },
      { kind: 'scalar', name: 'sequence' },
    ]);
  });

  it('rejects relation paths beyond the parser depth limit', () => {
    const cyclic = {
      tables: new Map([
        [
          'nodes',
          {
            name: 'nodes',
            columns: [{ name: 'id', type: 'integer' }],
            relations: [
              {
                propertyName: 'child',
                type: 'one-to-many',
                targetTableName: 'nodes',
              },
            ],
          },
        ],
      ]),
    };
    const path = `${Array.from({ length: 70 }, () => 'child').join('.')}.id`;
    expect(() =>
      parseFields([path], 'nodes', cyclic, new JoinRegistry()),
    ).toThrow(/field nesting depth/i);
  });

  it('correctly identifies relationType', () => {
    const { tree } = parse(['author.name', 'tags.label'], 'posts');
    const author = tree.nodes.find(
      (n) => n.kind === 'relation' && n.propertyName === 'author',
    );
    const tags = tree.nodes.find(
      (n) => n.kind === 'relation' && n.propertyName === 'tags',
    );
    expect((author as any).relationType).toBe('many-to-one');
    expect((tags as any).relationType).toBe('many-to-many');
  });
});
