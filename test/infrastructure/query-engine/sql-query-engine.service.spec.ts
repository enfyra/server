import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import Knex from 'knex';
import { SqlQueryEngine } from '../../../src/infrastructure/query-engine/services/sql-query-engine.service';
import { QueryBuilderService } from '../../../src/infrastructure/query-builder/query-builder.service';
import { MetadataCacheService } from '../../../src/infrastructure/cache/services/metadata-cache.service';
import { LoggingService } from '../../../src/core/exceptions/services/logging.service';

describe('SqlQueryEngine - Comprehensive Tests', () => {
  let service: SqlQueryEngine;
  let knex: Knex.Knex;
  let metadataCache: MetadataCacheService;

  beforeAll(async () => {
    // Setup SQLite in-memory database
    knex = Knex({
      client: 'sqlite3',
      connection: ':memory:',
      useNullAsDefault: true,
    });

    // Create test schema
    await setupTestSchema(knex);

    // Setup test module
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        SqlQueryEngine,
        {
          provide: QueryBuilderService,
          useValue: {
            getConnection: () => knex,
            executeForQueryEngine: jest.fn(),
          },
        },
        {
          provide: MetadataCacheService,
          useValue: {
            getTableMetadata: jest.fn(),
            getMetadata: jest.fn(),
          },
        },
        {
          provide: LoggingService,
          useValue: {
            error: jest.fn(),
          },
        },
        {
          provide: ConfigService,
          useValue: {
            get: (key: string) => (key === 'DB_TYPE' ? 'sqlite' : null),
          },
        },
      ],
    }).compile();

    service = module.get<SqlQueryEngine>(SqlQueryEngine);
    metadataCache = module.get<MetadataCacheService>(MetadataCacheService);

    // Setup metadata mock
    setupMetadataMock(metadataCache);
  });

  afterAll(async () => {
    await knex.destroy();
  });

  describe('Basic Query Operations', () => {
    it('should select all fields when no fields specified', async () => {
      const result = await service.find({
        tableName: 'users',
      });

      expect(result.data).toBeDefined();
      expect(result.data.length).toBeGreaterThan(0);
      expect(result.data[0]).toHaveProperty('id');
      expect(result.data[0]).toHaveProperty('name');
      expect(result.data[0]).toHaveProperty('email');
    });

    it('should select specific fields only', async () => {
      const result = await service.find({
        tableName: 'users',
        fields: ['id', 'name'],
      });

      expect(result.data[0]).toHaveProperty('id');
      expect(result.data[0]).toHaveProperty('name');
      expect(result.data[0]).not.toHaveProperty('email');
    });

    it('should handle pagination correctly', async () => {
      const page1 = await service.find({
        tableName: 'users',
        fields: ['id'],
        page: 1,
        limit: 2,
      });

      const page2 = await service.find({
        tableName: 'users',
        fields: ['id'],
        page: 2,
        limit: 2,
      });

      expect(page1.data.length).toBe(2);
      expect(page2.data.length).toBe(2);
      expect(page1.data[0].id).not.toBe(page2.data[0].id);
    });

    it('should handle sorting', async () => {
      const ascending = await service.find({
        tableName: 'users',
        fields: ['id', 'name'],
        sort: 'name',
      });

      const descending = await service.find({
        tableName: 'users',
        fields: ['id', 'name'],
        sort: '-name',
      });

      expect(ascending.data[0].name).toBeLessThan(
        ascending.data[ascending.data.length - 1].name,
      );
      expect(descending.data[0].name).toBeGreaterThan(
        descending.data[descending.data.length - 1].name,
      );
    });
  });

  describe('Filter Operations', () => {
    it('should filter with _eq operator', async () => {
      const result = await service.find({
        tableName: 'users',
        filter: { name: { _eq: 'Alice' } },
      });

      expect(result.data.length).toBe(1);
      expect(result.data[0].name).toBe('Alice');
    });

    it('should filter with _neq operator', async () => {
      const result = await service.find({
        tableName: 'users',
        filter: { name: { _neq: 'Alice' } },
      });

      expect(result.data.every((u) => u.name !== 'Alice')).toBe(true);
    });

    it('should filter with _in operator', async () => {
      const result = await service.find({
        tableName: 'users',
        filter: { name: { _in: ['Alice', 'Bob'] } },
      });

      expect(result.data.length).toBe(2);
      expect(result.data.map((u) => u.name).sort()).toEqual(['Alice', 'Bob']);
    });

    it('should filter with _contains operator', async () => {
      const result = await service.find({
        tableName: 'users',
        filter: { email: { _contains: 'example' } },
      });

      expect(result.data.every((u) => u.email.includes('example'))).toBe(true);
    });

    it('should filter with _and operator', async () => {
      const result = await service.find({
        tableName: 'users',
        filter: {
          _and: [{ name: { _contains: 'a' } }, { email: { _contains: 'example' } }],
        },
      });

      expect(
        result.data.every(
          (u) => u.name.toLowerCase().includes('a') && u.email.includes('example'),
        ),
      ).toBe(true);
    });

    it('should filter with _or operator', async () => {
      const result = await service.find({
        tableName: 'users',
        filter: {
          _or: [{ name: { _eq: 'Alice' } }, { name: { _eq: 'Bob' } }],
        },
      });

      expect(result.data.length).toBe(2);
    });
  });

  describe('Relation Operations', () => {
    it('should auto-load relation IDs when no fields specified', async () => {
      const result = await service.find({
        tableName: 'users',
        limit: 1,
      });

      expect(result.data[0]).toHaveProperty('posts');
      expect(Array.isArray(result.data[0].posts)).toBe(true);
      if (result.data[0].posts.length > 0) {
        expect(result.data[0].posts[0]).toHaveProperty('id');
      }
    });

    it('should load M2O relation with specific fields', async () => {
      const result = await service.find({
        tableName: 'posts',
        fields: ['id', 'title', 'author.name'],
        limit: 1,
      });

      expect(result.data[0]).toHaveProperty('author');
      expect(result.data[0].author).toHaveProperty('name');
    });

    it('should load O2M relation with wildcard', async () => {
      const result = await service.find({
        tableName: 'users',
        fields: ['id', 'name', 'posts.*'],
        limit: 1,
      });

      expect(result.data[0]).toHaveProperty('posts');
      if (result.data[0].posts.length > 0) {
        expect(result.data[0].posts[0]).toHaveProperty('id');
        expect(result.data[0].posts[0]).toHaveProperty('title');
        expect(result.data[0].posts[0]).toHaveProperty('content');
      }
    });

    it('should handle nested relations 3 levels deep', async () => {
      const result = await service.find({
        tableName: 'posts',
        fields: ['id', 'author.profile.bio'],
        limit: 1,
      });

      expect(result.data[0]).toHaveProperty('author');
      expect(result.data[0].author).toHaveProperty('profile');
      expect(result.data[0].author.profile).toHaveProperty('bio');
    });
  });

  describe('Filter on Relations', () => {
    it('should filter on M2O relation', async () => {
      const result = await service.find({
        tableName: 'posts',
        filter: { author: { name: { _eq: 'Alice' } } },
        fields: ['id', 'title'],
      });

      expect(result.data.length).toBeGreaterThan(0);
    });

    it('should filter on O2M relation', async () => {
      const result = await service.find({
        tableName: 'users',
        filter: { posts: { title: { _contains: 'Hello' } } },
        fields: ['id', 'name'],
      });

      expect(result.data.length).toBeGreaterThan(0);
    });

    it('should filter on nested M2O->O2M relation', async () => {
      const result = await service.find({
        tableName: 'posts',
        filter: { author: { posts: { title: { _contains: 'Test' } } } },
        fields: ['id', 'title'],
      });

      expect(result.data).toBeDefined();
    });

    it('should use DISTINCT when filtering on O2M', async () => {
      const result = await service.find({
        tableName: 'users',
        filter: { posts: { title: { _contains: 'Hello' } } },
        fields: ['id', 'name'],
        debugMode: true,
      });

      expect(result.debug.sql).toContain('distinct');
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty result set', async () => {
      const result = await service.find({
        tableName: 'users',
        filter: { name: { _eq: 'NonExistentUser' } },
      });

      expect(result.data).toEqual([]);
    });

    it('should handle null filter values', async () => {
      const result = await service.find({
        tableName: 'users',
        filter: { middleName: { _is_null: true } },
      });

      expect(result.data.length).toBeGreaterThan(0);
    });

    it('should handle very deep nesting (5 levels)', async () => {
      const result = await service.find({
        tableName: 'posts',
        fields: ['id', 'author.profile.country.region.name'],
        limit: 1,
      });

      expect(result.data).toBeDefined();
    });

    it('should handle complex nested filters with AND/OR', async () => {
      const result = await service.find({
        tableName: 'users',
        filter: {
          _and: [
            { name: { _contains: 'a' } },
            {
              _or: [
                { posts: { title: { _contains: 'Hello' } } },
                { posts: { title: { _contains: 'Test' } } },
              ],
            },
          ],
        },
        fields: ['id', 'name'],
      });

      expect(result.data).toBeDefined();
    });
  });

  describe('Meta Information', () => {
    it('should return totalCount when requested', async () => {
      const result = await service.find({
        tableName: 'users',
        meta: 'totalCount',
        limit: 2,
      });

      expect(result.meta).toHaveProperty('totalCount');
      expect(result.meta.totalCount).toBeGreaterThan(2);
    });

    it('should return filterCount when filter is applied', async () => {
      const result = await service.find({
        tableName: 'users',
        filter: { name: { _contains: 'a' } },
        meta: 'filterCount',
      });

      expect(result.meta).toHaveProperty('filterCount');
    });

    it('should return all meta when using wildcard', async () => {
      const result = await service.find({
        tableName: 'users',
        meta: '*',
        limit: 2,
      });

      expect(result.meta).toHaveProperty('totalCount');
      expect(result.meta).toHaveProperty('filterCount');
    });
  });

  describe('Debug Mode', () => {
    it('should return debug info when enabled', async () => {
      const result = await service.find({
        tableName: 'users',
        fields: ['id', 'name'],
        debugMode: true,
      });

      expect(result.debug).toBeDefined();
      expect(result.debug).toHaveProperty('sql');
      expect(result.debug).toHaveProperty('joinArr');
      expect(result.debug).toHaveProperty('selectArr');
    });
  });
});

// Helper functions
async function setupTestSchema(knex: Knex.Knex) {
  // Create users table
  await knex.schema.createTable('users', (table) => {
    table.increments('id').primary();
    table.string('name').notNullable();
    table.string('email').notNullable();
    table.string('middleName').nullable();
    table.timestamps(true, true);
  });

  // Create profiles table
  await knex.schema.createTable('profiles', (table) => {
    table.increments('id').primary();
    table.integer('userId').unsigned().notNullable();
    table.text('bio').nullable();
    table.integer('countryId').unsigned().nullable();
    table.foreign('userId').references('users.id');
    table.timestamps(true, true);
  });

  // Create posts table
  await knex.schema.createTable('posts', (table) => {
    table.increments('id').primary();
    table.string('title').notNullable();
    table.text('content').notNullable();
    table.integer('authorId').unsigned().notNullable();
    table.foreign('authorId').references('users.id');
    table.timestamps(true, true);
  });

  // Create countries table
  await knex.schema.createTable('countries', (table) => {
    table.increments('id').primary();
    table.string('name').notNullable();
    table.integer('regionId').unsigned().nullable();
    table.timestamps(true, true);
  });

  // Create regions table
  await knex.schema.createTable('regions', (table) => {
    table.increments('id').primary();
    table.string('name').notNullable();
    table.timestamps(true, true);
  });

  // Insert test data
  await insertTestData(knex);
}

async function insertTestData(knex: Knex.Knex) {
  // Insert regions
  await knex('regions').insert([
    { id: 1, name: 'Asia' },
    { id: 2, name: 'Europe' },
  ]);

  // Insert countries
  await knex('countries').insert([
    { id: 1, name: 'Vietnam', regionId: 1 },
    { id: 2, name: 'France', regionId: 2 },
  ]);

  // Insert users
  await knex('users').insert([
    { id: 1, name: 'Alice', email: 'alice@example.com', middleName: null },
    { id: 2, name: 'Bob', email: 'bob@example.com', middleName: 'John' },
    { id: 3, name: 'Charlie', email: 'charlie@example.com', middleName: null },
    { id: 4, name: 'David', email: 'david@example.com', middleName: 'Lee' },
  ]);

  // Insert profiles
  await knex('profiles').insert([
    { id: 1, userId: 1, bio: 'Alice bio', countryId: 1 },
    { id: 2, userId: 2, bio: 'Bob bio', countryId: 2 },
    { id: 3, userId: 3, bio: 'Charlie bio', countryId: 1 },
  ]);

  // Insert posts
  await knex('posts').insert([
    { id: 1, title: 'Hello World', content: 'First post', authorId: 1 },
    { id: 2, title: 'Test Post', content: 'Testing', authorId: 1 },
    { id: 3, title: 'Hello Again', content: 'Second post', authorId: 2 },
    { id: 4, title: 'Final Test', content: 'Last test', authorId: 3 },
  ]);
}

function setupMetadataMock(metadataCache: MetadataCacheService) {
  const metadata = {
    tables: new Map([
      [
        'users',
        {
          name: 'users',
          columns: [
            { name: 'id', type: 'int', isPrimary: true },
            { name: 'name', type: 'varchar' },
            { name: 'email', type: 'varchar' },
            { name: 'middleName', type: 'varchar', isNullable: true },
          ],
          relations: [
            {
              propertyName: 'posts',
              type: 'one-to-many',
              targetTableName: 'posts',
              inversePropertyName: 'author',
            },
            {
              propertyName: 'profile',
              type: 'one-to-one',
              targetTableName: 'profiles',
              inversePropertyName: 'user',
            },
          ],
        },
      ],
      [
        'posts',
        {
          name: 'posts',
          columns: [
            { name: 'id', type: 'int', isPrimary: true },
            { name: 'title', type: 'varchar' },
            { name: 'content', type: 'text' },
            { name: 'authorId', type: 'int' },
          ],
          relations: [
            {
              propertyName: 'author',
              type: 'many-to-one',
              targetTableName: 'users',
              foreignKeyColumn: 'authorId',
            },
          ],
        },
      ],
      [
        'profiles',
        {
          name: 'profiles',
          columns: [
            { name: 'id', type: 'int', isPrimary: true },
            { name: 'userId', type: 'int' },
            { name: 'bio', type: 'text' },
            { name: 'countryId', type: 'int' },
          ],
          relations: [
            {
              propertyName: 'user',
              type: 'many-to-one',
              targetTableName: 'users',
              foreignKeyColumn: 'userId',
            },
            {
              propertyName: 'country',
              type: 'many-to-one',
              targetTableName: 'countries',
              foreignKeyColumn: 'countryId',
            },
          ],
        },
      ],
      [
        'countries',
        {
          name: 'countries',
          columns: [
            { name: 'id', type: 'int', isPrimary: true },
            { name: 'name', type: 'varchar' },
            { name: 'regionId', type: 'int' },
          ],
          relations: [
            {
              propertyName: 'region',
              type: 'many-to-one',
              targetTableName: 'regions',
              foreignKeyColumn: 'regionId',
            },
          ],
        },
      ],
      [
        'regions',
        {
          name: 'regions',
          columns: [
            { name: 'id', type: 'int', isPrimary: true },
            { name: 'name', type: 'varchar' },
          ],
          relations: [],
        },
      ],
    ]),
  };

  jest.spyOn(metadataCache, 'getMetadata').mockResolvedValue(metadata as any);
  jest
    .spyOn(metadataCache, 'getTableMetadata')
    .mockImplementation((tableName) =>
      Promise.resolve(metadata.tables.get(tableName)),
    );
}
