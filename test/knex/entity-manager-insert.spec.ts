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
