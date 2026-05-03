import { describe, expect, it, vi } from 'vitest';
import { analyzeRelationChanges } from '../../src/engines/knex/utils/migration/relation-changes';

function createKnexMock(relationRows: any[] = []) {
  return vi.fn((table: string) => {
    if (table === 'table_definition') {
      return {
        select: vi.fn().mockReturnThis(),
        whereIn: vi.fn().mockResolvedValue([{ id: 2, name: 'students' }]),
      };
    }

    if (table === 'relation_definition') {
      return {
        where: vi.fn().mockReturnThis(),
        select: vi.fn().mockResolvedValue(relationRows),
      };
    }

    throw new Error(`Unexpected table ${table}`);
  }) as any;
}

function createDiff() {
  return {
    columns: { create: [], delete: [], rename: [] },
    constraints: { uniques: { create: [], delete: [] } },
    indexes: { create: [], delete: [] },
    crossTableOperations: [],
    junctionTables: { create: [], drop: [], update: [], rename: [] },
  };
}

describe('analyzeRelationChanges inverse relation handling', () => {
  it('does not drop a junction table when deleting an inverse relation', async () => {
    const diff = createDiff();

    await analyzeRelationChanges(
      createKnexMock(),
      [
        {
          id: 10,
          type: 'many-to-many',
          propertyName: 'students',
          mappedBy: 'tests',
          mappedById: 9,
          targetTable: { id: 2 },
          targetTableName: 'students',
          junctionTableName: 'test_students_students',
        },
      ],
      [],
      diff,
      'tests',
      [],
      [],
    );

    expect(diff.junctionTables.drop).toEqual([]);
  });

  it('drops a junction table when deleting the owning relation', async () => {
    const diff = createDiff();

    await analyzeRelationChanges(
      createKnexMock(),
      [
        {
          id: 9,
          type: 'many-to-many',
          propertyName: 'students',
          targetTable: { id: 2 },
          targetTableName: 'students',
          junctionTableName: 'test_students_students',
        },
      ],
      [],
      diff,
      'tests',
      [],
      [],
    );

    expect(diff.junctionTables.drop).toEqual([
      {
        tableName: 'test_students_students',
        reason: 'Relation deleted',
      },
    ]);
  });

  it('does not drop a junction table when another owning relation still references it', async () => {
    const diff = createDiff();

    await analyzeRelationChanges(
      createKnexMock([
        { id: 9, mappedById: null },
        { id: 10, mappedById: 9 },
      ]),
      [
        {
          id: 10,
          type: 'many-to-many',
          propertyName: 'courses',
          targetTable: { id: 2 },
          targetTableName: 'courses',
          junctionTableName: 'test_students_students',
        },
      ],
      [],
      diff,
      'students',
      [],
      [],
    );

    expect(diff.junctionTables.drop).toEqual([]);
  });

  it('does not delete a foreign key column when deleting an inverse one-to-one relation', async () => {
    const diff = createDiff();

    await analyzeRelationChanges(
      createKnexMock(),
      [
        {
          id: 20,
          type: 'one-to-one',
          propertyName: 'course',
          mappedBy: 'room',
          mappedById: 19,
          targetTable: { id: 2 },
          targetTableName: 'courses',
        },
      ],
      [],
      diff,
      'rooms',
      [],
      [],
    );

    expect(diff.columns.delete).toEqual([]);
  });

  it('does not rename a foreign key column when renaming an inverse one-to-one relation', async () => {
    const diff = createDiff();

    await analyzeRelationChanges(
      createKnexMock(),
      [
        {
          id: 20,
          type: 'one-to-one',
          propertyName: 'course',
          mappedBy: 'room',
          mappedById: 19,
          targetTable: { id: 2 },
          targetTableName: 'courses',
        },
      ],
      [
        {
          id: 20,
          type: 'one-to-one',
          propertyName: 'primaryCourse',
          mappedBy: 'room',
          mappedById: 19,
          targetTable: { id: 2 },
          targetTableName: 'courses',
        },
      ],
      diff,
      'rooms',
      [],
      [],
    );

    expect(diff.columns.rename).toEqual([]);
  });

  it('does not rename the junction table when renaming an owning many-to-many relation', async () => {
    const diff = createDiff();

    await analyzeRelationChanges(
      createKnexMock(),
      [
        {
          id: 30,
          type: 'many-to-many',
          propertyName: 'students',
          targetTable: { id: 2 },
          targetTableName: 'students',
          junctionTableName: 'course_students',
        },
      ],
      [
        {
          id: 30,
          type: 'many-to-many',
          propertyName: 'learners',
          targetTable: { id: 2 },
          targetTableName: 'students',
          junctionTableName: 'course_students',
        },
      ],
      diff,
      'courses',
      [],
      [],
    );

    expect(diff.junctionTables.rename).toEqual([]);
    expect(diff.junctionTables.drop).toEqual([]);
    expect(diff.junctionTables.create).toEqual([]);
  });
});
