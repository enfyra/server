import { describe, expect, it } from 'vitest';
import { SqlAggregateExecutor } from '../../../kernel/src/query/query-builder/executors/sql-aggregate-executor';
import { MongoQueryExecutor } from '../../../kernel/src/query/query-builder/executors/mongo-query-executor';

const metadata = { tables: new Map([['events', { columns: [
  { name: 'id', type: 'int', isPrimary: true },
  { name: 'day', type: 'date' },
], relations: [] }]]) };
const aggregate = { dimensions: [{ field: 'day', bucket: 'day', timezone: 'Asia/Ho_Chi_Minh' }], measures: { count: { count: 'id' } } };

describe('calendar date aggregation', () => {
  it('does not apply timezone conversion to a PostgreSQL calendar date', async () => {
    const fragments: string[] = [];
    const query: any = { select: () => query, groupByRaw: () => query, then: (resolve: (rows: unknown[]) => void) => resolve([]) };
    const knex: any = () => query;
    knex.raw = (sql: string) => { fragments.push(sql); return sql; };
    await new SqlAggregateExecutor(knex, 'postgres').aggregate({ tableName: 'events', aggregate, metadata } as any);
    expect(fragments.join(' ')).not.toContain('AT TIME ZONE');
  });

  it('converts a Mongo calendar string without applying the requested timezone', async () => {
    let pipeline: any[] = [];
    const db: any = { collection: () => ({ aggregate: (stages: any[]) => { pipeline = stages; return { toArray: async () => [] }; } }) };
    await new MongoQueryExecutor({ getDb: () => db } as any).aggregate({ tableName: 'events', aggregate, metadata });
    const expression = pipeline.find((stage) => stage.$group).$group._id.day;
    expect(expression.$dateTrunc.date).toEqual({ $dateFromString: { dateString: '$day', format: '%Y-%m-%d', timezone: 'UTC' } });
    expect(expression.$dateTrunc.timezone).toBe('UTC');
  });
});
