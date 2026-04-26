import { parseFields } from 'src/kernel/query';
import { JoinRegistry } from 'src/kernel/query';

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

  it('correctly identifies relationType', () => {
    const { tree } = parse(
      ['author.name', 'tags.label', 'posts.title'],
      'posts',
    );
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
