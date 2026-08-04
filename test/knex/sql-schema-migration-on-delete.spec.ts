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
      raw: vi
        .fn()
        .mockResolvedValue([[{ CONSTRAINT_NAME: 'fk_course_teacherId' }]]),
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

  it('drops an existing index by its physical name when metadata derived a different name', async () => {
    const knex = {
      raw: vi.fn(async (query: string) => {
        const normalizedQuery = query.toLowerCase();
        if (normalizedQuery.includes('information_schema.columns')) return [[]];
        if (normalizedQuery.includes('key_column_usage')) return [[]];
        if (normalizedQuery.includes('information_schema.statistics')) {
          return [
            [
              {
                indexName: 'idx_ai_gateway_models_modelId',
                isNonUnique: 1,
                columns: 'modelName,id',
              },
            ],
          ];
        }
        return [[]];
      }),
      schema: { hasColumn: vi.fn().mockResolvedValue(true) },
      client: { config: { client: 'mysql2' } },
    } as any;

    const statements = await generateSQLFromDiff(
      knex,
      'ai_gateway_models',
      { constraints: { indexes: { delete: [['modelName', 'id']] } } },
      'mysql',
    );

    expect(statements).toContain(
      'ALTER TABLE `ai_gateway_models` DROP INDEX `idx_ai_gateway_models_modelId`',
    );
    expect(statements).not.toContain(
      'ALTER TABLE `ai_gateway_models` DROP INDEX `idx_ai_gateway_models_modelName_id`',
    );
  });

  it('resolves the existing PostgreSQL index name by physical columns', async () => {
    const knex = {
      raw: vi.fn(async (query: string) => {
        const normalizedQuery = query.toLowerCase();
        if (normalizedQuery.includes('information_schema.columns')) {
          return { rows: [] };
        }
        if (normalizedQuery.includes('table_constraints')) {
          return { rows: [] };
        }
        if (normalizedQuery.includes('pg_index')) {
          return {
            rows: [
              {
                index_name: 'idx_ai_gateway_models_modelId',
                is_unique: false,
                columns: ['modelName', 'id'],
              },
            ],
          };
        }
        return { rows: [] };
      }),
      schema: { hasColumn: vi.fn().mockResolvedValue(true) },
      client: { config: { client: 'pg' } },
    } as any;

    const statements = await generateSQLFromDiff(
      knex,
      'ai_gateway_models',
      { constraints: { indexes: { delete: [['modelName', 'id']] } } },
      'postgres',
    );

    expect(statements).toContain(
      'DROP INDEX IF EXISTS "idx_ai_gateway_models_modelId"',
    );
    expect(statements).not.toContain(
      'DROP INDEX IF EXISTS "idx_ai_gateway_models_modelName_id"',
    );
  });

  it('fails planning before DDL when it cannot inspect an index slated for removal', async () => {
    const knex = {
      raw: vi.fn().mockRejectedValue(new Error('database connection lost')),
      schema: { hasColumn: vi.fn().mockResolvedValue(true) },
      client: { config: { client: 'pg' } },
    } as any;

    await expect(
      generateSQLFromDiff(
        knex,
        'ai_gateway_models',
        { constraints: { indexes: { delete: [['modelName', 'id']] } } },
        'postgres',
      ),
    ).rejects.toThrow(
      /Cannot safely plan index removal for ai_gateway_models\(modelName, id\)/,
    );
  });
});
