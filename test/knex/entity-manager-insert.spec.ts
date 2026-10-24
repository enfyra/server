import { describe, expect, it, vi } from 'vitest';
import { KnexEntityManager } from '../../src/engines/knex/entity-manager';

function createKnexMock(columnsByTable: Record<string, Record<string, any>>) {
  const calls: string[] = [];
  const knex = vi.fn((tableName: string) => ({
    columnInfo: vi.fn(async (columnName: string) => {
      calls.push(`${tableName}.columnInfo(${columnName})`);
      return columnsByTable[tableName]?.[columnName];
    }),
    insert: vi.fn(() => {
      calls.push(`${tableName}.insert`);
      return {
        returning: vi.fn(async (columnName: string) => {
          calls.push(`${tableName}.returning(${columnName})`);
          return [{ id: 123 }];
        }),
      };
    }),
  })) as any;

  return { knex, calls };
}

describe('KnexEntityManager.insertMany', () => {
  it('preserves generated IDs, explicit IDs, and per-row after hooks for MySQL batches', async () => {
    const insert = vi.fn(async () => [40]);
    const knex = vi.fn(() => ({ insert }));
    const afterInsert = vi.fn(async () => undefined);
    const manager = new KnexEntityManager(
      knex as any,
      { beforeInsert: [], afterInsert: [afterInsert] },
      'mysql',
    );
    const rows = [{ name: 'first' }, { id: 80, name: 'explicit' }, { name: 'last' }];

    await expect(manager.insertMany('batch_item', rows)).resolves.toEqual([40, 80, 42]);
    expect(insert).toHaveBeenCalledWith(rows);
    expect(afterInsert.mock.calls).toEqual([
      ['batch_item', 40],
      ['batch_item', 80],
      ['batch_item', 42],
    ]);
  });

  it('does not require a generated ID when every inserted row supplies its own ID', async () => {
    const knex = vi.fn(() => ({ insert: vi.fn(async () => []) }));
    const manager = new KnexEntityManager(
      knex as any,
      { beforeInsert: [], afterInsert: [] },
      'mysql',
    );

    await expect(manager.insertMany('batch_item', [{ id: 0 }, { id: 80 }])).resolves.toEqual([0, 80]);
  });

  it('preserves the missing-generated-ID fallback without skipping after-insert hooks', async () => {
    const knex = vi.fn(() => ({ insert: vi.fn(async () => []) }));
    const afterInsert = vi.fn();
    const manager = new KnexEntityManager(
      knex as any,
      { beforeInsert: [], afterInsert: [afterInsert] },
      'mysql',
    );

    await expect(
      manager.insertMany('batch_item', [{ name: 'first' }]),
    ).resolves.toEqual([Number.NaN]);
    expect(afterInsert).toHaveBeenCalledWith('batch_item', undefined);
  });
});

describe('KnexEntityManager.insert', () => {
  it('returns id for PostgreSQL tables with underscores when they have an id column', async () => {
    const { knex, calls } = createKnexMock({
      e2e_flow_teacher_123: { id: { type: 'integer' } },
    });
    const manager = new KnexEntityManager(
      knex,
      { beforeInsert: [], afterInsert: [] },
      'postgres',
    );

    const id = await manager.insert('e2e_flow_teacher_123', {
      name: 'Teacher A',
    });

    expect(id).toBe(123);
    expect(calls).toEqual([
      'e2e_flow_teacher_123.columnInfo(id)',
      'e2e_flow_teacher_123.insert',
      'e2e_flow_teacher_123.returning(id)',
    ]);
  });

  it('does not request returning id for PostgreSQL junction tables without an id column', async () => {
    const { knex, calls } = createKnexMock({
      course_students_student: {},
    });
    const manager = new KnexEntityManager(
      knex,
      { beforeInsert: [], afterInsert: [] },
      'postgres',
    );

    const id = await manager.insert('course_students_student', {
      courseId: 1,
      studentId: 2,
    });

    expect(id).toBeUndefined();
    expect(calls).toEqual([
      'course_students_student.columnInfo(id)',
      'course_students_student.insert',
    ]);
  });
});
