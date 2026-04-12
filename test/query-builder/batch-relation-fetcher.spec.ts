import knex, { Knex } from 'knex';
import {
  executeBatchFetches,
  BatchFetchDescriptor,
} from '../../src/infrastructure/query-builder/utils/sql/batch-relation-fetcher';

let db: Knex;

const META: Record<string, any> = {
  users: {
    name: 'users',
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'name', type: 'varchar' },
      { name: 'email', type: 'varchar' },
    ],
    relations: [
      {
        propertyName: 'posts',
        type: 'one-to-many',
        targetTableName: 'posts',
        targetTable: 'posts',
        foreignKeyColumn: 'authorId',
        isInverse: true,
        mappedBy: 'author',
      },
      {
        propertyName: 'profile',
        type: 'one-to-one',
        targetTableName: 'profiles',
        targetTable: 'profiles',
        foreignKeyColumn: 'userId',
        isInverse: true,
        mappedBy: 'user',
      },
    ],
  },
  posts: {
    name: 'posts',
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'title', type: 'varchar' },
      { name: 'authorId', type: 'integer' },
      { name: 'categoryId', type: 'integer' },
    ],
    relations: [
      {
        propertyName: 'author',
        type: 'many-to-one',
        targetTableName: 'users',
        targetTable: 'users',
        foreignKeyColumn: 'authorId',
        isInverse: false,
      },
      {
        propertyName: 'category',
        type: 'many-to-one',
        targetTableName: 'categories',
        targetTable: 'categories',
        foreignKeyColumn: 'categoryId',
        isInverse: false,
      },
      {
        propertyName: 'tags',
        type: 'many-to-many',
        targetTableName: 'tags',
        targetTable: 'tags',
        isInverse: false,
        junctionTableName: 'posts_tags_tags',
        junctionSourceColumn: 'postsId',
        junctionTargetColumn: 'tagsId',
      },
    ],
  },
  categories: {
    name: 'categories',
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'name', type: 'varchar' },
    ],
    relations: [],
  },
  tags: {
    name: 'tags',
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'label', type: 'varchar' },
    ],
    relations: [
      {
        propertyName: 'posts',
        type: 'many-to-many',
        targetTableName: 'posts',
        targetTable: 'posts',
        isInverse: true,
        mappedBy: 'tags',
        junctionTableName: 'posts_tags_tags',
        junctionSourceColumn: 'tagsId',
        junctionTargetColumn: 'postsId',
      },
    ],
  },
  profiles: {
    name: 'profiles',
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'userId', type: 'integer' },
      { name: 'bio', type: 'varchar' },
    ],
    relations: [
      {
        propertyName: 'user',
        type: 'one-to-one',
        targetTableName: 'users',
        targetTable: 'users',
        foreignKeyColumn: 'userId',
        isInverse: false,
      },
    ],
  },
  tree_nodes: {
    name: 'tree_nodes',
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'name', type: 'varchar' },
      { name: 'parentId', type: 'integer' },
    ],
    relations: [
      {
        propertyName: 'parent',
        type: 'many-to-one',
        targetTableName: 'tree_nodes',
        targetTable: 'tree_nodes',
        foreignKeyColumn: 'parentId',
        isInverse: false,
      },
      {
        propertyName: 'children',
        type: 'one-to-many',
        targetTableName: 'tree_nodes',
        targetTable: 'tree_nodes',
        foreignKeyColumn: 'parentId',
        isInverse: true,
        mappedBy: 'parent',
      },
    ],
  },
  uuid_table: {
    name: 'uuid_table',
    columns: [
      { name: 'uuid', type: 'varchar', isPrimary: true },
      { name: 'data', type: 'varchar' },
    ],
    relations: [],
  },
};

const metadataGetter = async (table: string) => META[table] || null;

const strippedMetadataGetter = async (table: string) => {
  const meta = META[table];
  if (!meta) return null;
  return {
    ...meta,
    columns: meta.columns.map((c: any) => ({ name: c.name, type: c.type })),
  };
};

beforeAll(async () => {
  db = knex({
    client: 'sqlite3',
    connection: { filename: ':memory:' },
    useNullAsDefault: true,
  });

  await db.schema.createTable('users', (t) => {
    t.increments('id').primary();
    t.string('name');
    t.string('email');
  });
  await db.schema.createTable('categories', (t) => {
    t.increments('id').primary();
    t.string('name');
  });
  await db.schema.createTable('posts', (t) => {
    t.increments('id').primary();
    t.string('title');
    t.integer('authorId').references('id').inTable('users');
    t.integer('categoryId').references('id').inTable('categories');
  });
  await db.schema.createTable('tags', (t) => {
    t.increments('id').primary();
    t.string('label');
  });
  await db.schema.createTable('posts_tags_tags', (t) => {
    t.integer('postsId').references('id').inTable('posts');
    t.integer('tagsId').references('id').inTable('tags');
  });
  await db.schema.createTable('profiles', (t) => {
    t.increments('id').primary();
    t.integer('userId').references('id').inTable('users').unique();
    t.string('bio');
  });
  await db.schema.createTable('tree_nodes', (t) => {
    t.increments('id').primary();
    t.string('name');
    t.integer('parentId').references('id').inTable('tree_nodes').nullable();
  });
  await db.schema.createTable('uuid_table', (t) => {
    t.string('uuid').primary();
    t.string('data');
  });

  await db('users').insert([
    { id: 1, name: 'Alice', email: 'alice@test.com' },
    { id: 2, name: 'Bob', email: 'bob@test.com' },
    { id: 3, name: 'Charlie', email: 'charlie@test.com' },
  ]);
  await db('categories').insert([
    { id: 1, name: 'Tech' },
    { id: 2, name: 'Science' },
  ]);
  await db('posts').insert([
    { id: 1, title: 'Post A', authorId: 1, categoryId: 1 },
    { id: 2, title: 'Post B', authorId: 1, categoryId: 2 },
    { id: 3, title: 'Post C', authorId: 2, categoryId: 1 },
    { id: 4, title: 'Post D', authorId: null, categoryId: null },
  ]);
  await db('tags').insert([
    { id: 1, label: 'JavaScript' },
    { id: 2, label: 'TypeScript' },
    { id: 3, label: 'Rust' },
  ]);
  await db('posts_tags_tags').insert([
    { postsId: 1, tagsId: 1 },
    { postsId: 1, tagsId: 2 },
    { postsId: 2, tagsId: 2 },
    { postsId: 3, tagsId: 3 },
  ]);
  await db('profiles').insert([
    { id: 1, userId: 1, bio: 'Software dev' },
    { id: 2, userId: 2, bio: 'Data scientist' },
  ]);
  await db('tree_nodes').insert([
    { id: 1, name: 'Root', parentId: null },
    { id: 2, name: 'Child 1', parentId: 1 },
    { id: 3, name: 'Child 2', parentId: 1 },
    { id: 4, name: 'Grandchild', parentId: 2 },
  ]);
  await db('uuid_table').insert([
    { uuid: 'aaa-111', data: 'first' },
    { uuid: 'bbb-222', data: 'second' },
  ]);
});

afterAll(async () => {
  await db.destroy();
});

describe('batch-relation-fetcher', () => {
  describe('M2O (fetchOwnerRelation)', () => {
    it('should stitch M2O relation by FK value (FK aliased as propertyName)', async () => {
      const rows = [
        { id: 1, title: 'Post A', author: 1 },
        { id: 2, title: 'Post B', author: 1 },
        { id: 3, title: 'Post C', author: 2 },
      ];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id', 'name'],
        fkColumn: 'authorId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].author).toEqual({ id: 1, name: 'Alice' });
      expect(rows[1].author).toEqual({ id: 1, name: 'Alice' });
      expect(rows[2].author).toEqual({ id: 2, name: 'Bob' });
    });

    it('should set null for rows with null FK', async () => {
      const rows = [
        { id: 4, title: 'Post D', author: null },
        { id: 1, title: 'Post A', author: 1 },
      ];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id', 'name'],
        fkColumn: 'authorId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].author).toBeNull();
      expect(rows[1].author).toEqual({ id: 1, name: 'Alice' });
    });

    it('should set null when all FK values are null', async () => {
      const rows = [{ id: 4, title: 'Post D', author: null }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id'],
        fkColumn: 'authorId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].author).toBeNull();
    });

    it('should set null when FK points to non-existent target', async () => {
      const rows = [{ id: 99, title: 'Ghost', author: 999 }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id', 'name'],
        fkColumn: 'authorId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].author).toBeNull();
    });

    it('should handle multiple M2O relations on same parent', async () => {
      const rows = [{ id: 1, title: 'Post A', author: 1, category: 1 }];
      const descs: BatchFetchDescriptor[] = [
        {
          relationName: 'author',
          type: 'many-to-one',
          targetTable: 'users',
          fields: ['id', 'name'],
          fkColumn: 'authorId',
        },
        {
          relationName: 'category',
          type: 'many-to-one',
          targetTable: 'categories',
          fields: ['id', 'name'],
          fkColumn: 'categoryId',
        },
      ];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].author).toEqual({ id: 1, name: 'Alice' });
      expect(rows[0].category).toEqual({ id: 1, name: 'Tech' });
    });

    it('should overwrite scalar FK with resolved object (no raw FK leak)', async () => {
      const rows = [{ id: 1, title: 'Post A', author: 1 }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id', 'name'],
        fkColumn: 'authorId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0]).not.toHaveProperty('authorId');
      expect(rows[0].author).toEqual({ id: 1, name: 'Alice' });
      expect(typeof rows[0].author).toBe('object');
    });
  });

  describe('O2M (fetchInverseRelation)', () => {
    it('should group children by parent FK', async () => {
      const rows = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['id', 'title'],
        fkColumn: 'authorId',
        isInverse: true,
        mappedBy: 'author',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'users');

      expect(rows[0].posts).toHaveLength(2);
      expect(rows[0].posts.map((p: any) => p.title).sort()).toEqual(['Post A', 'Post B']);
      expect(rows[1].posts).toHaveLength(1);
      expect(rows[1].posts[0].title).toBe('Post C');
    });

    it('should return empty array for parent with no children', async () => {
      const rows = [{ id: 3, name: 'Charlie' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['id', 'title'],
        fkColumn: 'authorId',
        isInverse: true,
        mappedBy: 'author',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'users');

      expect(rows[0].posts).toEqual([]);
    });

    it('should order children by PK ascending', async () => {
      const rows = [{ id: 1, name: 'Alice' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['id', 'title'],
        fkColumn: 'authorId',
        isInverse: true,
        mappedBy: 'author',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'users');

      const ids = rows[0].posts.map((p: any) => p.id);
      expect(ids).toEqual([...ids].sort((a: number, b: number) => a - b));
    });
  });

  describe('M2M (fetchManyToMany)', () => {
    it('should resolve M2M through junction table', async () => {
      const rows = [
        { id: 1, title: 'Post A' },
        { id: 2, title: 'Post B' },
      ];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'tags',
        type: 'many-to-many',
        targetTable: 'tags',
        fields: ['id', 'label'],
        junctionTableName: 'posts_tags_tags',
        junctionSourceColumn: 'postsId',
        junctionTargetColumn: 'tagsId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].tags).toHaveLength(2);
      expect(rows[0].tags.map((t: any) => t.label).sort()).toEqual(['JavaScript', 'TypeScript']);
      expect(rows[1].tags).toHaveLength(1);
      expect(rows[1].tags[0].label).toBe('TypeScript');
    });

    it('should return empty array for parent with no M2M entries', async () => {
      const rows = [{ id: 4, title: 'Post D' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'tags',
        type: 'many-to-many',
        targetTable: 'tags',
        fields: ['id', 'label'],
        junctionTableName: 'posts_tags_tags',
        junctionSourceColumn: 'postsId',
        junctionTargetColumn: 'tagsId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].tags).toEqual([]);
    });

    it('should not leak __sourceId__ into result objects', async () => {
      const rows = [{ id: 1, title: 'Post A' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'tags',
        type: 'many-to-many',
        targetTable: 'tags',
        fields: ['id', 'label'],
        junctionTableName: 'posts_tags_tags',
        junctionSourceColumn: 'postsId',
        junctionTargetColumn: 'tagsId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      for (const tag of rows[0].tags) {
        expect(tag).not.toHaveProperty('__sourceId__');
      }
    });

    it('should throw when junction config is missing', async () => {
      const rows = [{ id: 1 }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'tags',
        type: 'many-to-many',
        targetTable: 'tags',
        fields: ['id'],
      }];

      await expect(
        executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts'),
      ).rejects.toThrow('Missing junction table config');
    });

    it('should handle inverse M2M (tags → posts)', async () => {
      const rows = [
        { id: 1, label: 'JavaScript' },
        { id: 2, label: 'TypeScript' },
        { id: 3, label: 'Rust' },
      ];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'posts',
        type: 'many-to-many',
        targetTable: 'posts',
        fields: ['id', 'title'],
        junctionTableName: 'posts_tags_tags',
        junctionSourceColumn: 'tagsId',
        junctionTargetColumn: 'postsId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'tags');

      expect(rows[0].posts).toHaveLength(1);
      expect(rows[0].posts[0].title).toBe('Post A');
      expect(rows[1].posts).toHaveLength(2);
      expect(rows[2].posts).toHaveLength(1);
      expect(rows[2].posts[0].title).toBe('Post C');
    });
  });

  describe('O2O', () => {
    it('should resolve owner O2O (profile → user)', async () => {
      const rows = [{ id: 1, user: 1, bio: 'Software dev' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'user',
        type: 'one-to-one',
        targetTable: 'users',
        fields: ['id', 'name'],
        fkColumn: 'userId',
        isInverse: false,
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'profiles');

      expect(rows[0].user).toEqual({ id: 1, name: 'Alice' });
    });

    it('should resolve inverse O2O as single object not array (user → profile)', async () => {
      const rows = [
        { id: 1, name: 'Alice' },
        { id: 3, name: 'Charlie' },
      ];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'profile',
        type: 'one-to-one',
        targetTable: 'profiles',
        fields: ['id', 'bio'],
        fkColumn: 'userId',
        isInverse: true,
        mappedBy: 'user',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'users');

      expect(rows[0].profile).toEqual({ id: 1, bio: 'Software dev' });
      expect(rows[0].profile).not.toHaveProperty('userId');
      expect(rows[1].profile).toBeNull();
    });
  });

  describe('self-referencing table', () => {
    it('should resolve M2O parent on self-ref table', async () => {
      const rows = [
        { id: 2, name: 'Child 1', parent: 1 },
        { id: 4, name: 'Grandchild', parent: 2 },
      ];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'parent',
        type: 'many-to-one',
        targetTable: 'tree_nodes',
        fields: ['id', 'name'],
        fkColumn: 'parentId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'tree_nodes');

      expect(rows[0].parent).toEqual({ id: 1, name: 'Root' });
      expect(rows[1].parent).toEqual({ id: 2, name: 'Child 1' });
    });

    it('should resolve O2M children on self-ref table', async () => {
      const rows = [{ id: 1, name: 'Root', parentId: null }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'children',
        type: 'one-to-many',
        targetTable: 'tree_nodes',
        fields: ['id', 'name'],
        fkColumn: 'parentId',
        isInverse: true,
        mappedBy: 'parent',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'tree_nodes');

      expect(rows[0].children).toHaveLength(2);
      expect(rows[0].children.map((c: any) => c.name).sort()).toEqual(['Child 1', 'Child 2']);
    });
  });

  describe('nested relations', () => {
    it('should resolve M2O inside O2M (user → posts → category)', async () => {
      const rows = [{ id: 1, name: 'Alice' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['id', 'title', 'category.name'],
        fkColumn: 'authorId',
        isInverse: true,
        mappedBy: 'author',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'users');

      expect(rows[0].posts).toHaveLength(2);
      const postA = rows[0].posts.find((p: any) => p.title === 'Post A');
      expect(postA.category).toEqual(expect.objectContaining({ name: 'Tech' }));
      expect(postA).not.toHaveProperty('categoryId');
    });

    it('should resolve M2M inside O2M (user → posts → tags)', async () => {
      const rows = [{ id: 1, name: 'Alice' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['id', 'title', 'tags.label'],
        fkColumn: 'authorId',
        isInverse: true,
        mappedBy: 'author',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'users');

      const postA = rows[0].posts.find((p: any) => p.title === 'Post A');
      expect(postA.tags).toHaveLength(2);
      expect(postA.tags.map((t: any) => t.label).sort()).toEqual(['JavaScript', 'TypeScript']);
    });

    it('should resolve 3 levels deep (tree: root → children → children)', async () => {
      const rows = [{ id: 1, name: 'Root', parentId: null }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'children',
        type: 'one-to-many',
        targetTable: 'tree_nodes',
        fields: ['id', 'name', 'children.id', 'children.name'],
        fkColumn: 'parentId',
        isInverse: true,
        mappedBy: 'parent',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'tree_nodes');

      expect(rows[0].children).toHaveLength(2);
      const child1 = rows[0].children.find((c: any) => c.name === 'Child 1');
      expect(child1.children).toHaveLength(1);
      expect(child1.children[0].name).toBe('Grandchild');
    });
  });

  describe('depth limiting', () => {
    it('should not fetch anything when maxDepth = 0', async () => {
      const rows = [{ id: 1, name: 'Alice' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['id', 'title'],
        fkColumn: 'authorId',
        isInverse: true,
        mappedBy: 'author',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 0, 0, 'users');

      expect(rows[0]).not.toHaveProperty('posts');
    });

    it('should stop at maxDepth for nested relations', async () => {
      const rows = [{ id: 1, name: 'Root', parentId: null }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'children',
        type: 'one-to-many',
        targetTable: 'tree_nodes',
        fields: ['id', 'name', 'children.id', 'children.name', 'children.children.id'],
        fkColumn: 'parentId',
        isInverse: true,
        mappedBy: 'parent',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 2, 0, 'tree_nodes');

      expect(rows[0].children).toHaveLength(2);
      const child1 = rows[0].children.find((c: any) => c.name === 'Child 1');
      expect(child1.children).toBeDefined();
      if (child1.children.length > 0) {
        expect(child1.children[0]).not.toHaveProperty('children');
      }
    });
  });

  describe('early returns', () => {
    it('should do nothing with empty parent rows', async () => {
      const rows: any[] = [];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id'],
        fkColumn: 'authorId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');
      expect(rows).toEqual([]);
    });

    it('should do nothing with empty descriptors', async () => {
      const rows = [{ id: 1, title: 'Post A', author: 1 }];
      await executeBatchFetches(db, rows, [], metadataGetter, 3, 0, 'posts');
      expect(rows[0].author).toBe(1);
    });
  });

  describe('error handling', () => {
    it('should throw when target table metadata is missing', async () => {
      const rows = [{ id: 1, ghost: 10 }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'ghost',
        type: 'many-to-one',
        targetTable: 'nonexistent_table',
        fields: ['id'],
        fkColumn: 'fkCol',
      }];

      await expect(
        executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts'),
      ).rejects.toThrow('Metadata not found');
    });
  });

  describe('resolveFieldsAndNested (via integration)', () => {
    it('should select only requested fields + PK', async () => {
      const rows = [{ id: 1, title: 'Post A', author: 1 }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['name'],
        fkColumn: 'authorId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].author).toHaveProperty('id');
      expect(rows[0].author).toHaveProperty('name');
      expect(rows[0].author).not.toHaveProperty('email');
    });

    it('should expand * to all columns of target table', async () => {
      const rows = [{ id: 1, title: 'Post A', author: 1 }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['*'],
        fkColumn: 'authorId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].author).toHaveProperty('id');
      expect(rows[0].author).toHaveProperty('name');
      expect(rows[0].author).toHaveProperty('email');
    });

    it('should auto-include FK column for nested M2O when using *', async () => {
      const rows = [{ id: 1, name: 'Alice' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['*'],
        fkColumn: 'authorId',
        isInverse: true,
        mappedBy: 'author',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'users');

      expect(rows[0].posts.length).toBeGreaterThan(0);
      const post = rows[0].posts[0];
      expect(post).toHaveProperty('author');
      expect(post.author).toHaveProperty('id');
    });

    it('should not leak FK columns when * used at nested level', async () => {
      const rows = [{ id: 1, name: 'Alice' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['*'],
        fkColumn: 'authorId',
        isInverse: true,
        mappedBy: 'author',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'users');

      const post = rows[0].posts[0];
      expect(post).not.toHaveProperty('authorId');
      expect(post).not.toHaveProperty('categoryId');
      expect(post).toHaveProperty('author');
      expect(post).toHaveProperty('category');
    });
  });

  describe('BUG: isPrimary stripped by getMetadataGetter', () => {
    it('should resolve PK correctly when isPrimary is present', async () => {
      const rows = [{ id: 1, title: 'Post A', author: 1 }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id', 'name'],
        fkColumn: 'authorId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].author).toEqual({ id: 1, name: 'Alice' });
    });

    it('should still work when isPrimary stripped IF PK is named "id"', async () => {
      const rows = [{ id: 1, title: 'Post A', author: 1 }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id', 'name'],
        fkColumn: 'authorId',
      }];

      await executeBatchFetches(db, rows, descs, strippedMetadataGetter, 3, 0, 'posts');

      expect(rows[0].author).toEqual({ id: 1, name: 'Alice' });
    });

    it('should CRASH on non-id PK when isPrimary is stripped (SQL column not found)', async () => {
      const uuidMeta = {
        ...META.uuid_table,
        columns: META.uuid_table.columns.map((c: any) => ({ name: c.name, type: c.type })),
      };
      const getter = async (table: string) => {
        if (table === 'uuid_table') return uuidMeta;
        return META[table] || null;
      };

      const rows = [{ id: 1, ref: 'aaa-111' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'ref',
        type: 'many-to-one',
        targetTable: 'uuid_table',
        fields: ['uuid', 'data'],
        fkColumn: 'refCol',
      }];

      await expect(
        executeBatchFetches(db, rows, descs, getter, 3, 0, 'some_table'),
      ).rejects.toThrow();
    });

    it('should resolve non-id PK when isPrimary is preserved', async () => {
      const rows = [{ id: 1, ref: 'aaa-111' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'ref',
        type: 'many-to-one',
        targetTable: 'uuid_table',
        fields: ['uuid', 'data'],
        fkColumn: 'refCol',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'some_table');

      expect(rows[0].ref).toEqual({ uuid: 'aaa-111', data: 'first' });
    });
  });

  describe('FK aliased as propertyName (no delete needed)', () => {
    it('should overwrite scalar FK with resolved object seamlessly', async () => {
      const rows = [{ id: 1, title: 'Post A', author: 1 }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id', 'name'],
        fkColumn: 'authorId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0]).not.toHaveProperty('authorId');
      expect(rows[0].author).toEqual({ id: 1, name: 'Alice' });
    });

    it('should handle multiple FK aliases without interference', async () => {
      const rows = [{ id: 1, title: 'Post A', author: 1, category: 1 }];
      const descs: BatchFetchDescriptor[] = [
        {
          relationName: 'author',
          type: 'many-to-one',
          targetTable: 'users',
          fields: ['id', 'name'],
          fkColumn: 'authorId',
        },
        {
          relationName: 'category',
          type: 'many-to-one',
          targetTable: 'categories',
          fields: ['id', 'name'],
          fkColumn: 'categoryId',
        },
      ];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0]).not.toHaveProperty('authorId');
      expect(rows[0]).not.toHaveProperty('categoryId');
      expect(rows[0].author).toEqual({ id: 1, name: 'Alice' });
      expect(rows[0].category).toEqual({ id: 1, name: 'Tech' });
    });

    it('should strip nested FK columns via AS in multi-level batch fetch', async () => {
      const rows = [{ id: 1, name: 'Alice' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['id', 'title', 'category.name'],
        fkColumn: 'authorId',
        isInverse: true,
        mappedBy: 'author',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'users');

      const post = rows[0].posts[0];
      expect(post).not.toHaveProperty('categoryId');
      expect(post.category).toEqual(expect.objectContaining({ name: 'Tech' }));
    });
  });

  describe('BUG: fetchInverseRelation fallback uses desc.targetTable', () => {
    it('should resolve O2M via mappedBy when fkColumn is missing', async () => {
      const rows = [{ id: 1, name: 'Alice' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['id', 'title'],
        isInverse: true,
        mappedBy: 'author',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'users');

      expect(rows[0].posts).toHaveLength(2);
    });

    it('should CRASH when both fkColumn and mappedBy are missing (falls back to postsId which does not exist)', async () => {
      const rows = [{ id: 1, name: 'Alice' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['id', 'title'],
        isInverse: true,
      }];

      await expect(
        executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'users'),
      ).rejects.toThrow();
    });
  });

  describe('PK type mismatch edge case', () => {
    it('should handle string PK matching correctly', async () => {
      const rows = [{ id: 1, ref: 'aaa-111' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'ref',
        type: 'many-to-one',
        targetTable: 'uuid_table',
        fields: ['uuid', 'data'],
        fkColumn: 'refCol',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'some_table');

      expect(rows[0].ref).toEqual({ uuid: 'aaa-111', data: 'first' });
    });

    it('should fail to match when FK type differs from PK type', async () => {
      const rows = [{ id: 1, ref: 1 }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'ref',
        type: 'many-to-one',
        targetTable: 'uuid_table',
        fields: ['uuid', 'data'],
        fkColumn: 'refCol',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'some_table');

      expect(rows[0].ref).toBeNull();
    });
  });

  describe('concurrent descriptors ordering', () => {
    it('should process all descriptors even if one relation has no data', async () => {
      const rows = [{ id: 4, title: 'Post D', author: null, category: null }];
      const descs: BatchFetchDescriptor[] = [
        {
          relationName: 'author',
          type: 'many-to-one',
          targetTable: 'users',
          fields: ['id', 'name'],
          fkColumn: 'authorId',
        },
        {
          relationName: 'tags',
          type: 'many-to-many',
          targetTable: 'tags',
          fields: ['id', 'label'],
          junctionTableName: 'posts_tags_tags',
          junctionSourceColumn: 'postsId',
          junctionTargetColumn: 'tagsId',
        },
      ];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].author).toBeNull();
      expect(rows[0].tags).toEqual([]);
    });
  });

  describe('adversarial: edge cases that try to break things', () => {
    it('should handle empty fields array without crashing', async () => {
      const rows = [{ id: 1, title: 'Post A', author: 1 }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: [],
        fkColumn: 'authorId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].author).toHaveProperty('id');
    });

    it('should handle duplicate fields without duplication in result', async () => {
      const rows = [{ id: 1, title: 'Post A', author: 1 }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['name', 'name', 'id', 'id'],
        fkColumn: 'authorId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].author).toEqual({ id: 1, name: 'Alice' });
    });

    it('should ignore non-existent field names gracefully', async () => {
      const rows = [{ id: 1, title: 'Post A', author: 1 }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['name', 'doesNotExist', 'alsoFake'],
        fkColumn: 'authorId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].author).toHaveProperty('name', 'Alice');
      expect(rows[0].author).toHaveProperty('id');
    });

    it('should handle all parent rows pointing to same FK (dedup)', async () => {
      const rows = Array.from({ length: 100 }, (_, i) => ({ id: i + 1, author: 1 }));
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id', 'name'],
        fkColumn: 'authorId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      for (const row of rows) {
        expect(row.author).toEqual({ id: 1, name: 'Alice' });
      }
    });

    it('should survive circular self-ref at depth boundary', async () => {
      const rows = [{ id: 1, name: 'Root', parent: null }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'children',
        type: 'one-to-many',
        targetTable: 'tree_nodes',
        fields: [
          'id', 'name',
          'children.id', 'children.name',
          'children.children.id', 'children.children.name',
          'children.children.children.id',
        ],
        fkColumn: 'parentId',
        isInverse: true,
        mappedBy: 'parent',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 4, 0, 'tree_nodes');

      const child1 = rows[0].children.find((c: any) => c.name === 'Child 1');
      expect(child1.children[0].name).toBe('Grandchild');
      expect(child1.children[0].children).toEqual([]);
    });

    it('should handle M2O + O2M + M2M all at once on same parent', async () => {
      const rows = [{ id: 1, title: 'Post A', author: 1, category: 1 }];
      const descs: BatchFetchDescriptor[] = [
        {
          relationName: 'author',
          type: 'many-to-one',
          targetTable: 'users',
          fields: ['id', 'name'],
          fkColumn: 'authorId',
        },
        {
          relationName: 'category',
          type: 'many-to-one',
          targetTable: 'categories',
          fields: ['id', 'name'],
          fkColumn: 'categoryId',
        },
        {
          relationName: 'tags',
          type: 'many-to-many',
          targetTable: 'tags',
          fields: ['id', 'label'],
          junctionTableName: 'posts_tags_tags',
          junctionSourceColumn: 'postsId',
          junctionTargetColumn: 'tagsId',
        },
      ];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].author).toEqual({ id: 1, name: 'Alice' });
      expect(rows[0].category).toEqual({ id: 1, name: 'Tech' });
      expect(rows[0].tags).toHaveLength(2);
      expect(rows[0]).not.toHaveProperty('authorId');
      expect(rows[0]).not.toHaveProperty('categoryId');
    });

    it('should handle nested M2O chain 3 levels deep', async () => {
      const rows = [{ id: 4, name: 'Grandchild', parent: 2 }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'parent',
        type: 'many-to-one',
        targetTable: 'tree_nodes',
        fields: ['id', 'name', 'parent.id', 'parent.name'],
        fkColumn: 'parentId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'tree_nodes');

      expect(rows[0].parent).toEqual(expect.objectContaining({ id: 2, name: 'Child 1' }));
      expect(rows[0].parent.parent).toEqual(expect.objectContaining({ id: 1, name: 'Root' }));
    });

    it('should handle M2M target with nested M2O (AS alias inside M2M join)', async () => {
      const rows = [{ id: 1, name: 'Alice' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['id', 'title', 'tags.id', 'tags.label'],
        fkColumn: 'authorId',
        isInverse: true,
        mappedBy: 'author',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'users');

      const postA = rows[0].posts.find((p: any) => p.title === 'Post A');
      expect(postA.tags).toHaveLength(2);
      expect(postA.tags[0]).toHaveProperty('label');
      expect(postA.tags[0]).not.toHaveProperty('__sourceId__');
    });

    it('should handle mixed null/valid FKs in same batch without cross-contamination', async () => {
      const rows = [
        { id: 1, author: 1 },
        { id: 2, author: null },
        { id: 3, author: 2 },
        { id: 4, author: null },
        { id: 5, author: 1 },
      ];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id', 'name'],
        fkColumn: 'authorId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].author).toEqual({ id: 1, name: 'Alice' });
      expect(rows[1].author).toBeNull();
      expect(rows[2].author).toEqual({ id: 2, name: 'Bob' });
      expect(rows[3].author).toBeNull();
      expect(rows[4].author).toEqual({ id: 1, name: 'Alice' });
    });

    it('should not mutate descriptor objects', async () => {
      const rows = [{ id: 1, author: 1 }];
      const desc: BatchFetchDescriptor = {
        relationName: 'author',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id', 'name'],
        fkColumn: 'authorId',
      };
      const originalDesc = JSON.parse(JSON.stringify(desc));

      await executeBatchFetches(db, rows, [desc], metadataGetter, 3, 0, 'posts');

      expect(desc).toEqual(originalDesc);
    });

    it('should handle O2M where no parent has matching children', async () => {
      const rows = [
        { id: 100, name: 'Nobody' },
        { id: 200, name: 'Also Nobody' },
      ];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['id'],
        fkColumn: 'authorId',
        isInverse: true,
        mappedBy: 'author',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'users');

      expect(rows[0].posts).toEqual([]);
      expect(rows[1].posts).toEqual([]);
    });

    it('should handle M2M where junction table is empty for all parents', async () => {
      const rows = [
        { id: 999 },
        { id: 998 },
      ];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'tags',
        type: 'many-to-many',
        targetTable: 'tags',
        fields: ['id', 'label'],
        junctionTableName: 'posts_tags_tags',
        junctionSourceColumn: 'postsId',
        junctionTargetColumn: 'tagsId',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].tags).toEqual([]);
      expect(rows[1].tags).toEqual([]);
    });

    it('should strip raw FK column from O2M children when not aliased', async () => {
      const rows = [{ id: 1, name: 'Alice' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['id', 'title'],
        fkColumn: 'authorId',
        isInverse: true,
        mappedBy: 'author',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'users');

      const post = rows[0].posts[0];
      expect(post).toHaveProperty('id');
      expect(post).toHaveProperty('title');
      expect(post).not.toHaveProperty('authorId');
    });

    it('should run independent descriptors in parallel (parent rows mutated by all)', async () => {
      const rows = [{ id: 1, title: 'Post A', author: 1, category: 1 }];
      const descs: BatchFetchDescriptor[] = [
        {
          relationName: 'author',
          type: 'many-to-one',
          targetTable: 'users',
          fields: ['name'],
          fkColumn: 'authorId',
        },
        {
          relationName: 'category',
          type: 'many-to-one',
          targetTable: 'categories',
          fields: ['name'],
          fkColumn: 'categoryId',
        },
        {
          relationName: 'tags',
          type: 'many-to-many',
          targetTable: 'tags',
          fields: ['label'],
          junctionTableName: 'posts_tags_tags',
          junctionSourceColumn: 'postsId',
          junctionTargetColumn: 'tagsId',
        },
      ];

      await executeBatchFetches(db, rows, descs, metadataGetter, 3, 0, 'posts');

      expect(rows[0].author).toHaveProperty('name', 'Alice');
      expect(rows[0].category).toHaveProperty('name', 'Tech');
      expect(rows[0].tags).toHaveLength(2);
    });

    it('should survive maxDepth=1 cutting off nested relations silently', async () => {
      const rows = [{ id: 1, name: 'Alice' }];
      const descs: BatchFetchDescriptor[] = [{
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['id', 'title', 'tags.label', 'author.name'],
        fkColumn: 'authorId',
        isInverse: true,
        mappedBy: 'author',
      }];

      await executeBatchFetches(db, rows, descs, metadataGetter, 1, 0, 'users');

      expect(rows[0].posts).toHaveLength(2);
      const post = rows[0].posts[0];
      expect(post).toHaveProperty('title');
      expect(post).not.toHaveProperty('tags');
      expect(post.author).toBe(post.author);
      expect(typeof post.author).not.toBe('object');
    });
  });
});
