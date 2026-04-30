import knex, { type Knex } from 'knex';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { SqlQueryExecutor } from '@enfyra/kernel';
import {
  SqlTableMetadataBuilderService,
  SqlTableMetadataWriterService,
} from '../../src/modules/table-management';
import { DatabaseConfigService } from '../../src/shared/services';

describe('SQL metadata mutation to query chain', () => {
  let db: Knex;

  beforeAll(async () => {
    DatabaseConfigService.overrideForTesting('mysql');
    db = knex({
      client: 'sqlite3',
      connection: { filename: ':memory:' },
      useNullAsDefault: true,
    });

    await db.schema.createTable('table_definition', (t) => {
      t.increments('id').primary();
      t.string('name').notNullable();
      t.boolean('isSystem').defaultTo(false);
      t.text('uniques');
      t.text('indexes');
      t.string('alias');
      t.text('description');
      t.boolean('isSingleRecord');
      t.boolean('validateBody');
    });

    await db.schema.createTable('column_definition', (t) => {
      t.increments('id').primary();
      t.string('name').notNullable();
      t.string('type').notNullable();
      t.boolean('isPrimary').defaultTo(false);
      t.boolean('isGenerated').defaultTo(false);
      t.boolean('isNullable').defaultTo(true);
      t.boolean('isSystem').defaultTo(false);
      t.boolean('isUpdatable').defaultTo(true);
      t.boolean('isPublished').defaultTo(true);
      t.text('defaultValue');
      t.text('options');
      t.text('description');
      t.text('placeholder');
      t.text('metadata');
      t.integer('tableId').notNullable();
    });

    await db.schema.createTable('relation_definition', (t) => {
      t.increments('id').primary();
      t.string('propertyName').notNullable();
      t.string('type').notNullable();
      t.integer('sourceTableId').notNullable();
      t.integer('targetTableId').notNullable();
      t.integer('mappedById');
      t.boolean('isNullable').defaultTo(true);
      t.boolean('isSystem').defaultTo(false);
      t.boolean('isUpdatable').defaultTo(true);
      t.boolean('isPublished').defaultTo(true);
      t.string('onDelete');
      t.text('description');
      t.string('junctionTableName');
      t.string('junctionSourceColumn');
      t.string('junctionTargetColumn');
    });

    await db.schema.createTable('column_rule_definition', (t) => {
      t.increments('id').primary();
      t.string('ruleType');
      t.integer('columnId');
      t.integer('relationId');
      t.text('options');
      t.boolean('isEnabled').defaultTo(true);
    });

    await db.schema.createTable('field_permission_definition', (t) => {
      t.increments('id').primary();
      t.integer('columnId');
      t.integer('relationId');
      t.string('action');
      t.boolean('isEnabled').defaultTo(true);
    });

    await db('table_definition').insert([
      { id: 1, name: 'authors', isSystem: false, uniques: '[]', indexes: '[]' },
      { id: 2, name: 'posts', isSystem: false, uniques: '[]', indexes: '[]' },
    ]);

    await db('column_definition').insert([
      {
        id: 10,
        tableId: 1,
        name: 'id',
        type: 'int',
        isPrimary: true,
        isGenerated: true,
        isNullable: false,
      },
      { id: 11, tableId: 1, name: 'name', type: 'varchar', isNullable: false },
      {
        id: 20,
        tableId: 2,
        name: 'id',
        type: 'int',
        isPrimary: true,
        isGenerated: true,
        isNullable: false,
      },
      { id: 21, tableId: 2, name: 'title', type: 'varchar', isNullable: false },
      { id: 22, tableId: 2, name: 'authorId', type: 'int', isNullable: true },
    ]);

    await db.schema.createTable('authors', (t) => {
      t.increments('id').primary();
      t.string('name').notNullable();
    });
    await db.schema.createTable('posts', (t) => {
      t.increments('id').primary();
      t.string('title').notNullable();
      t.integer('authorId');
    });

    await db('authors').insert([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]);
    await db('posts').insert([
      { id: 1, title: 'First post', authorId: 1 },
      { id: 2, title: 'Second post', authorId: 2 },
      { id: 3, title: 'Draft post', authorId: null },
    ]);
  });

  afterAll(async () => {
    DatabaseConfigService.resetForTesting();
    await db.destroy();
  });

  it('persists inverse relation metadata, reloads metadata, then filters through the relation', async () => {
    const writer = new SqlTableMetadataWriterService();
    const affectedTables = new Set<string>();

    await writer.writeTableMetadataUpdates(
      db,
      2,
      {
        name: 'posts',
        columns: [
          { id: 20, name: 'id', type: 'int', isPrimary: true, isGenerated: true },
          { id: 21, name: 'title', type: 'varchar', isNullable: false },
          { id: 22, name: 'authorId', type: 'int', isNullable: true },
        ],
        relations: [
          {
            propertyName: 'author',
            type: 'many-to-one',
            targetTable: { id: 1 },
            inversePropertyName: 'posts',
            isNullable: true,
            onDelete: 'SET NULL',
          },
        ],
      },
      { id: 2, name: 'posts', uniques: '[]', indexes: '[]' },
      affectedTables,
    );

    const relationRows = await db('relation_definition').orderBy('id', 'asc');
    const owning = relationRows.find((row) => row.propertyName === 'author');
    const inverse = relationRows.find((row) => row.propertyName === 'posts');

    expect(owning).toMatchObject({
      sourceTableId: 2,
      targetTableId: 1,
      type: 'many-to-one',
      onDelete: 'SET NULL',
    });
    expect(inverse).toMatchObject({
      sourceTableId: 1,
      targetTableId: 2,
      type: 'one-to-many',
      mappedById: owning.id,
      onDelete: 'SET NULL',
    });
    expect([...affectedTables]).toEqual(['authors']);

    const builder = new SqlTableMetadataBuilderService({
      queryBuilderService: {} as any,
      metadataCacheService: {} as any,
    });
    const postsMeta = await builder.getFullTableMetadataInTransaction(db, 2);
    const authorsMeta = await builder.getFullTableMetadataInTransaction(db, 1);
    const metadata = {
      tables: new Map([
        ['posts', postsMeta],
        ['authors', authorsMeta],
      ]),
      tablesList: [postsMeta, authorsMeta],
    };

    const executor = new SqlQueryExecutor(db, 'sqlite');
    const result = await executor.execute({
      tableName: 'posts',
      fields: ['id', 'title', 'author.name'],
      filter: { author: { name: { _eq: 'Alice' } } },
      sort: 'id',
      metadata,
    });

    expect(result.data).toEqual([
      {
        id: 1,
        title: 'First post',
        author: { id: 1, name: 'Alice' },
      },
    ]);
  });
});
