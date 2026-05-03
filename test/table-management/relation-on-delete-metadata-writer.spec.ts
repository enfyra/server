import { describe, expect, it } from 'vitest';
import { SqlTableMetadataWriterService } from '../../src/modules/table-management';

function createQueryRunner(existingRelations: Record<number, any> = {}) {
  const inserts: Record<string, any[]> = {};
  let nextRelationId = 700;

  const runner: any = (table: string) => {
    const state: any = { table, whereValue: null, whereInValue: null };
    const resolveRows = () => {
      if (table === 'relation_definition') {
        if (state.whereValue?.sourceTableId === 1) return [];
        if (state.whereValue?.sourceTableId === 2) return [];
      }
      if (table === 'table_definition' && state.whereInValue?.column === 'id') {
        return [
          { id: 1, name: 'courses' },
          { id: 2, name: 'users' },
        ].filter((row) => state.whereInValue.values.includes(row.id));
      }
      return [];
    };
    const builder: any = {
      where(value: any) {
        state.whereValue = value;
        return builder;
      },
      whereIn(column: string, values: any[]) {
        state.whereInValue = { column, values };
        return builder;
      },
      select() {
        return builder;
      },
      first() {
        if (
          table === 'relation_definition' &&
          state.whereValue?.sourceTableId !== undefined &&
          state.whereValue?.propertyName !== undefined
        ) {
          const relation = Object.values(existingRelations).find(
            (row: any) =>
              row.sourceTableId === state.whereValue.sourceTableId &&
              row.propertyName === state.whereValue.propertyName,
          );
          return Promise.resolve(relation ?? null);
        }
        if (
          table === 'relation_definition' &&
          state.whereValue?.sourceTableId === 2 &&
          state.whereValue?.propertyName === 'posts'
        ) {
          return Promise.resolve(null);
        }
        if (
          table === 'relation_definition' &&
          state.whereValue?.id === 701
        ) {
          return Promise.resolve(inserts.relation_definition?.[0] ?? null);
        }
        if (
          table === 'relation_definition' &&
          existingRelations[state.whereValue?.id]
        ) {
          return Promise.resolve(existingRelations[state.whereValue.id]);
        }
        return Promise.resolve(null);
      },
      update(data: any) {
        inserts[table] = inserts[table] || [];
        inserts[table].push({ __update: true, ...data });
        return Promise.resolve(1);
      },
      delete() {
        return Promise.resolve(1);
      },
      insert(data: any) {
        const rows = Array.isArray(data) ? data : [data];
        inserts[table] = inserts[table] || [];
        for (const row of rows) {
          inserts[table].push({ ...row });
        }
        if (table === 'relation_definition') {
          return Promise.resolve([nextRelationId++]);
        }
        return Promise.resolve([1]);
      },
      then(resolve: any, reject: any) {
        return Promise.resolve(resolveRows()).then(resolve, reject);
      },
    };
    return builder;
  };

  return { runner, inserts };
}

describe('SqlTableMetadataWriterService relation onDelete metadata', () => {
  it('persists onDelete for owning and inverse relations', async () => {
    const { runner, inserts } = createQueryRunner();
    const service = new SqlTableMetadataWriterService();

    await service.writeTableMetadataUpdates(
      runner,
      1,
      {
        name: 'posts',
        columns: [],
        relations: [
          {
            propertyName: 'author',
            type: 'many-to-one',
            targetTable: { id: 2 },
            inversePropertyName: 'posts',
            isNullable: false,
            onDelete: 'CASCADE',
          },
        ],
      },
      { id: 1, name: 'posts', uniques: '[]', indexes: '[]' },
      new Set<string>(),
    );

    const relationRows = inserts.relation_definition || [];
    const owning = relationRows.find((row) => row.propertyName === 'author');
    const inverse = relationRows.find((row) => row.propertyName === 'posts');

    expect(owning?.onDelete).toBe('CASCADE');
    expect(inverse?.onDelete).toBe('CASCADE');
  });

  it('normalizes string target table IDs before resolving target metadata', async () => {
    const { runner, inserts } = createQueryRunner();
    const service = new SqlTableMetadataWriterService();

    await service.writeTableMetadataUpdates(
      runner,
      1,
      {
        name: 'posts',
        columns: [],
        relations: [
          {
            propertyName: 'likedBy',
            type: 'many-to-many',
            targetTable: { id: '2' as any },
          },
        ],
      },
      { id: 1, name: 'posts', uniques: '[]', indexes: '[]' },
      new Set<string>(),
    );

    const relationRows = inserts.relation_definition || [];
    const relation = relationRows.find((row) => row.propertyName === 'likedBy');

    expect(relation?.targetTableId).toBe(2);
    expect(relation?.junctionTableName).toBeTruthy();
  });

  it('preserves mappedById when updating an inverse relation without remapping it', async () => {
    const { runner, inserts } = createQueryRunner({
      801: {
        id: 801,
        propertyName: 'courses',
        type: 'many-to-many',
        targetTableId: 1,
        mappedById: 700,
        junctionTableName: 'course_students',
        junctionSourceColumn: 'studentId',
        junctionTargetColumn: 'courseId',
      },
    });
    const service = new SqlTableMetadataWriterService();

    await service.writeTableMetadataUpdates(
      runner,
      2,
      {
        name: 'students',
        columns: [],
        relations: [
          {
            id: 801,
            propertyName: 'enrolledCourses',
            type: 'many-to-many',
            targetTable: { id: 1 },
          },
        ],
      },
      { id: 2, name: 'students', uniques: '[]', indexes: '[]' },
      new Set<string>(),
    );

    const relationRows = inserts.relation_definition || [];
    const update = relationRows.find((row) => row.__update);

    expect(update?.mappedById).toBe(700);
    expect(update?.junctionTableName).toBe('course_students');
    expect(update?.junctionSourceColumn).toBe('studentId');
    expect(update?.junctionTargetColumn).toBe('courseId');
  });

  it('copies owning FK metadata when creating an inverse relation via mappedBy', async () => {
    const { runner, inserts } = createQueryRunner({
      701: {
        id: 701,
        sourceTableId: 1,
        propertyName: 'mentor',
        type: 'many-to-one',
        targetTableId: 2,
        foreignKeyColumn: 'teacherId',
        referencedColumn: 'id',
        constraintName: 'fk_courses_teacher',
      },
    });
    const service = new SqlTableMetadataWriterService();

    await service.writeTableMetadataUpdates(
      runner,
      2,
      {
        name: 'teachers',
        columns: [],
        relations: [
          {
            propertyName: 'mentoredCourses',
            type: 'one-to-many',
            targetTable: { id: 1 },
            mappedBy: 'mentor',
          },
        ],
      },
      { id: 2, name: 'teachers', uniques: '[]', indexes: '[]' },
      new Set<string>(),
    );

    const relationRows = inserts.relation_definition || [];
    const inverse = relationRows.find((row) => row.propertyName === 'mentoredCourses');

    expect(inverse?.mappedById).toBe(701);
    expect(inverse?.foreignKeyColumn).toBe('teacherId');
    expect(inverse?.referencedColumn).toBe('id');
    expect(inverse?.constraintName).toBe('fk_courses_teacher');
  });
});
