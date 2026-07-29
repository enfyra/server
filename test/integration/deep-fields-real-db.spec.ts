import knex, { Knex } from 'knex';
import { SqlQueryExecutor } from '@enfyra/kernel';

const DEFAULT_PG = 'postgresql://root:1234@127.0.0.1:5432/postgres';
const PREFIX = `__deep_${Date.now()}_`;
const T = {
  workOrders: `${PREFIX}work_orders`,
  inspections: `${PREFIX}inspections`,
  inspectors: `${PREFIX}inspectors`,
  tags: `${PREFIX}tags`,
  workOrderTags: `${PREFIX}work_order_tags`,
};

const metadata = {
  tables: new Map<string, any>([
    [
      T.workOrders,
      {
        name: T.workOrders,
        columns: [
          { name: 'id', type: 'integer', isPrimary: true },
          { name: 'number', type: 'varchar' },
        ],
        relations: [
          {
            propertyName: 'inspections',
            type: 'one-to-many',
            targetTableName: T.inspections,
            targetTable: T.inspections,
            foreignKeyColumn: 'workOrderId',
            mappedBy: 'workOrder',
            isInverse: true,
          },
          {
            propertyName: 'tags',
            type: 'many-to-many',
            targetTableName: T.tags,
            targetTable: T.tags,
            junctionTableName: T.workOrderTags,
            junctionSourceColumn: 'workOrderId',
            junctionTargetColumn: 'tagId',
            isInverse: false,
          },
        ],
      },
    ],
    [
      T.inspections,
      {
        name: T.inspections,
        columns: [
          { name: 'id', type: 'integer', isPrimary: true },
          { name: 'result', type: 'varchar' },
          { name: 'seq', type: 'integer' },
          { name: 'workOrderId', type: 'integer' },
          { name: 'inspectorId', type: 'integer' },
        ],
        relations: [
          {
            propertyName: 'workOrder',
            type: 'many-to-one',
            targetTableName: T.workOrders,
            targetTable: T.workOrders,
            foreignKeyColumn: 'workOrderId',
            isInverse: false,
          },
          {
            propertyName: 'inspector',
            type: 'many-to-one',
            targetTableName: T.inspectors,
            targetTable: T.inspectors,
            foreignKeyColumn: 'inspectorId',
            isInverse: false,
          },
        ],
      },
    ],
    [
      T.inspectors,
      {
        name: T.inspectors,
        columns: [
          { name: 'id', type: 'integer', isPrimary: true },
          { name: 'name', type: 'varchar' },
        ],
        relations: [],
      },
    ],
    [
      T.tags,
      {
        name: T.tags,
        columns: [
          { name: 'id', type: 'integer', isPrimary: true },
          { name: 'label', type: 'varchar' },
          { name: 'priority', type: 'integer' },
        ],
        relations: [],
      },
    ],
  ]),
};

describe('deep field semantics real DB', () => {
  let db: Knex;
  let available = true;
  let executor: SqlQueryExecutor;

  beforeAll(async () => {
    db = knex({
      client: 'pg',
      connection: process.env.PG_TEST_URI || DEFAULT_PG,
      pool: { min: 0, max: 4 },
    });

    try {
      await db.raw('SELECT 1');
    } catch {
      available = false;
      return;
    }

    await db.schema.dropTableIfExists(T.workOrderTags);
    await db.schema.dropTableIfExists(T.inspections);
    await db.schema.dropTableIfExists(T.workOrders);
    await db.schema.dropTableIfExists(T.inspectors);
    await db.schema.dropTableIfExists(T.tags);

    await db.schema.createTable(T.workOrders, (t) => {
      t.increments('id').primary();
      t.string('number');
    });
    await db.schema.createTable(T.inspectors, (t) => {
      t.increments('id').primary();
      t.string('name');
    });
    await db.schema.createTable(T.tags, (t) => {
      t.increments('id').primary();
      t.string('label');
      t.integer('priority');
    });
    await db.schema.createTable(T.inspections, (t) => {
      t.increments('id').primary();
      t.string('result');
      t.integer('seq');
      t.integer('workOrderId');
      t.integer('inspectorId');
    });
    await db.schema.createTable(T.workOrderTags, (t) => {
      t.integer('workOrderId');
      t.integer('tagId');
    });

    await db(T.workOrders).insert([
      { id: 1, number: 'WO-1' },
      { id: 2, number: 'WO-2' },
    ]);
    await db(T.inspectors).insert([
      { id: 10, name: 'Ada' },
      { id: 11, name: 'Grace' },
    ]);
    await db(T.tags).insert([
      { id: 200, label: 'urgent', priority: 3 },
      { id: 201, label: 'quality', priority: 2 },
      { id: 202, label: 'archive', priority: 1 },
    ]);
    await db(T.inspections).insert([
      { id: 100, result: 'failed', seq: 1, workOrderId: 1, inspectorId: 10 },
      { id: 101, result: 'passed', seq: 2, workOrderId: 1, inspectorId: 11 },
      { id: 102, result: 'passed', seq: 1, workOrderId: 2, inspectorId: null },
    ]);
    await db(T.workOrderTags).insert([
      { workOrderId: 1, tagId: 200 },
      { workOrderId: 1, tagId: 201 },
      { workOrderId: 1, tagId: 202 },
      { workOrderId: 2, tagId: 201 },
    ]);

    executor = new SqlQueryExecutor(db, 'postgres');
  }, 30000);

  afterAll(async () => {
    if (!db) return;
    try {
      if (available) {
        await db.schema.dropTableIfExists(T.workOrderTags);
        await db.schema.dropTableIfExists(T.inspections);
        await db.schema.dropTableIfExists(T.workOrders);
        await db.schema.dropTableIfExists(T.inspectors);
        await db.schema.dropTableIfExists(T.tags);
      }
    } finally {
      await db.destroy();
    }
  }, 30000);

  test('omitted root fields behaves like wildcard and loads one relation level', async () => {
    if (!available) return;

    const result = await executor.execute({
      tableName: T.workOrders,
      sort: 'id',
      limit: 1,
      metadata,
    });

    expect(result.data[0]).toMatchObject({
      id: 1,
      number: 'WO-1',
      inspections: [{ id: 100 }, { id: 101 }],
    });
  });

  test('deep relation without fields behaves like wildcard in that relation scope', async () => {
    if (!available) return;

    const result = await executor.execute({
      tableName: T.workOrders,
      fields: ['id', 'number'],
      deep: {
        inspections: {
          sort: 'seq',
          limit: 1,
        },
      },
      sort: 'id',
      limit: 1,
      metadata,
    });

    expect(result.data[0]).toMatchObject({
      id: 1,
      number: 'WO-1',
      inspections: [
        {
          id: 100,
          result: 'failed',
          seq: 1,
          inspector: { id: 10 },
          workOrder: { id: 1 },
        },
      ],
    });
    expect(result.data[0].inspections[0]).not.toHaveProperty('workOrderId');
    expect(result.data[0].inspections[0]).not.toHaveProperty('inspectorId');
  });

  test('nested deep can request a relation not present in parent fields', async () => {
    if (!available) return;

    const result = await executor.execute({
      tableName: T.workOrders,
      fields: ['id'],
      deep: {
        inspections: {
          fields: ['id', 'result'],
          limit: 1,
          deep: {
            inspector: { fields: ['name'] },
          },
        },
      },
      sort: 'id',
      limit: 1,
      metadata,
    });

    expect(result.data[0]).toEqual({
      id: 1,
      inspections: [
        {
          id: 100,
          result: 'failed',
          inspector: { id: 10, name: 'Ada' },
        },
      ],
    });
  });

  test('custom picked deep fields do not auto-load unrelated child relations', async () => {
    if (!available) return;

    const result = await executor.execute({
      tableName: T.workOrders,
      fields: ['id'],
      deep: {
        inspections: {
          fields: ['id', 'result'],
          sort: '-seq',
          limit: 2,
        },
      },
      sort: 'id',
      limit: 1,
      metadata,
    });

    expect(result.data[0]).toEqual({
      id: 1,
      inspections: [
        { id: 101, result: 'passed' },
        { id: 100, result: 'failed' },
      ],
    });
  });

  test('children wildcard auto-loads one relation level in the child scope', async () => {
    if (!available) return;

    const result = await executor.execute({
      tableName: T.workOrders,
      fields: ['id', 'inspections.*'],
      sort: 'id',
      limit: 1,
      metadata,
    });

    expect(result.data[0].inspections[0]).toMatchObject({
      id: 100,
      result: 'failed',
      seq: 1,
      inspector: { id: 10 },
      workOrder: { id: 1 },
    });
    expect(result.data[0].inspections[0]).not.toHaveProperty('workOrderId');
    expect(result.data[0].inspections[0]).not.toHaveProperty('inspectorId');
  });

  test('many-to-one root wildcard auto-loads owner relations as lightweight refs', async () => {
    if (!available) return;

    const result = await executor.execute({
      tableName: T.inspections,
      sort: 'id',
      limit: 1,
      metadata,
    });

    expect(result.data[0]).toMatchObject({
      id: 100,
      result: 'failed',
      seq: 1,
      workOrder: { id: 1 },
      inspector: { id: 10 },
    });
    expect(result.data[0]).not.toHaveProperty('workOrderId');
    expect(result.data[0]).not.toHaveProperty('inspectorId');
  });

  test('many-to-one null FK stays null through wildcard relation loading', async () => {
    if (!available) return;

    const result = await executor.execute({
      tableName: T.inspections,
      filter: { id: { _eq: 102 } },
      metadata,
    });

    expect(result.data[0]).toMatchObject({
      id: 102,
      result: 'passed',
      inspector: null,
      workOrder: { id: 2 },
    });
  });

  test('many-to-many deep without fields defaults to wildcard with sort, filter, limit, and page', async () => {
    if (!available) return;

    const result = await executor.execute({
      tableName: T.workOrders,
      fields: ['id'],
      deep: {
        tags: {
          filter: { priority: { _gte: 2 } },
          sort: '-priority',
          limit: 1,
          page: 2,
        },
      },
      filter: { id: { _eq: 1 } },
      metadata,
    });

    expect(result.data[0]).toEqual({
      id: 1,
      tags: [{ id: 201, label: 'quality', priority: 2 }],
    });
  });

  test('multiple deep relations load together without clobbering each other', async () => {
    if (!available) return;

    const result = await executor.execute({
      tableName: T.workOrders,
      fields: ['id', 'number'],
      deep: {
        inspections: {
          fields: ['id', 'result'],
          sort: 'seq',
          limit: 1,
          deep: { inspector: { fields: ['name'] } },
        },
        tags: {
          fields: ['label'],
          sort: 'priority',
          limit: 2,
        },
      },
      filter: { id: { _eq: 1 } },
      metadata,
    });

    expect(result.data[0]).toEqual({
      id: 1,
      number: 'WO-1',
      inspections: [
        { id: 100, result: 'failed', inspector: { id: 10, name: 'Ada' } },
      ],
      tags: [
        { id: 202, label: 'archive' },
        { id: 201, label: 'quality' },
      ],
    });
  });

  test('meta counts and relation loading compose on the same read', async () => {
    if (!available) return;

    const result = await executor.execute({
      tableName: T.workOrders,
      filter: { id: { _gte: 1 } },
      fields: ['id'],
      deep: { inspections: { fields: ['id'], limit: 1 } },
      sort: '-id',
      limit: 1,
      meta: '*',
      metadata,
    });

    expect(result.meta).toMatchObject({ filterCount: 2, totalCount: 2 });
    expect(result.data).toEqual([{ id: 2, inspections: [{ id: 102 }] }]);
  });
});
