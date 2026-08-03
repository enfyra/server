import { describe, expect, it, vi } from 'vitest';
import { resolveSqlRelationOnDelete } from '../../src/engines/knex/services/sql-schema-migration.service';
import { generateSQLFromDiff } from '../../src/engines/knex/utils/migration/sql-diff-generator';

describe('resolveSqlRelationOnDelete', () => {
  it('preserves explicit CASCADE for non-null relations', () => {
    expect(
      resolveSqlRelationOnDelete({
        onDelete: 'CASCADE',
        isNullable: false,
      }),
    ).toBe('CASCADE');
  });

  it('preserves explicit SET NULL and RESTRICT actions', () => {
    expect(
      resolveSqlRelationOnDelete({
        onDelete: 'SET NULL',
        isNullable: true,
      }),
    ).toBe('SET NULL');
    expect(
      resolveSqlRelationOnDelete({
        onDelete: 'RESTRICT',
        isNullable: true,
      }),
    ).toBe('RESTRICT');
  });

  it('falls back to nullability only when onDelete is missing', () => {
    expect(resolveSqlRelationOnDelete({ isNullable: false })).toBe('RESTRICT');
    expect(resolveSqlRelationOnDelete({ isNullable: 0 })).toBe('RESTRICT');
    expect(resolveSqlRelationOnDelete({ isNullable: true })).toBe('SET NULL');
    expect(resolveSqlRelationOnDelete({})).toBe('SET NULL');
  });

  it('drops a MySQL SET NULL foreign key before making its column non-null', async () => {
    const knex = {
      raw: vi.fn().mockResolvedValue([
        [{ CONSTRAINT_NAME: 'fk_course_teacherId' }],
      ]),
      schema: { hasColumn: vi.fn().mockResolvedValue(true) },
    } as any;
    const statements = await generateSQLFromDiff(
      knex,
      'course',
      {
        columns: {
          update: [
            {
              oldColumn: {
                name: 'teacherId',
                type: 'int',
                isNullable: true,
              },
              newColumn: {
                name: 'teacherId',
                type: 'int',
                isNullable: false,
              },
            },
          ],
        },
        foreignKeys: {
          recreate: [
            {
              tableName: 'course',
              columnName: 'teacherId',
              targetTable: 'teacher',
              targetColumn: 'id',
              onDelete: 'CASCADE',
            },
          ],
        },
      },
      'mysql',
    );

    const dropIndex = statements.findIndex((sql) =>
      sql.includes('DROP FOREIGN KEY'),
    );
    const modifyIndex = statements.findIndex((sql) =>
      sql.includes('MODIFY COLUMN'),
    );
    const addIndex = statements.findIndex((sql) =>
      sql.includes('ADD CONSTRAINT'),
    );
    expect(dropIndex).toBeLessThan(modifyIndex);
    expect(modifyIndex).toBeLessThan(addIndex);
  });
});
