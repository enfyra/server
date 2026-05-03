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

describe('analyzeRelationChanges inverse many-to-many junction handling', () => {
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
});
