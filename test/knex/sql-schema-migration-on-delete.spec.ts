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

  it('drops a MySQL foreign key before its supporting index', async () => {
    const knex = {
      raw: vi.fn(async (query: string) => {
        const normalizedQuery = query.toLowerCase();
        if (normalizedQuery.includes('key_column_usage')) {
          return [[{ CONSTRAINT_NAME: 'fk_course_teacherId' }]];
        }
        if (normalizedQuery.includes('information_schema.statistics')) {
          return [
            [
              {
                indexName: 'idx_course_teacherId_id',
                isNonUnique: 1,
                columns: 'teacherId,id',
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
      'course',
      {
        columns: { delete: [{ name: 'teacherId', isForeignKey: true }] },
        constraints: { indexes: { delete: [['teacherId', 'id']] } },
      },
      'mysql',
    );

    const dropForeignKeyIndex = statements.findIndex((sql) =>
      sql.includes('DROP FOREIGN KEY'),
    );
    const dropIndexIndex = statements.findIndex((sql) =>
      sql.includes('DROP INDEX'),
    );
    expect(dropForeignKeyIndex).toBeLessThan(dropIndexIndex);
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

  it('drops an existing PostgreSQL unique constraint by its physical name', async () => {
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
                index_name: 'uq_ai_gateway_models_upstreamModel',
                is_unique: true,
                columns: ['upstreamModel'],
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
      { constraints: { uniques: { delete: [['upstreamModel']] } } },
      'postgres',
    );

    expect(statements).toContain(
      'ALTER TABLE "ai_gateway_models" DROP CONSTRAINT "uq_ai_gateway_models_upstreamModel"',
    );
  });

  it('drops PostgreSQL unique constraints before their columns', async () => {
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
                index_name: 'course_name_key',
                is_unique: true,
                columns: ['name'],
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
      'course',
      {
        columns: { delete: [{ name: 'name' }] },
        constraints: { uniques: { delete: [['name']] } },
      },
      'postgres',
    );

    expect(
      statements.findIndex((statement) =>
        statement.includes('DROP CONSTRAINT "course_name_key"'),
      ),
    ).toBeLessThan(
      statements.findIndex((statement) =>
        statement.includes('DROP COLUMN IF EXISTS "name"'),
      ),
    );
  });

  it.each([
    ['postgres', '"'],
    ['mysql', '`'],
  ] as const)(
    'emits one %s unique constraint when an owning one-to-one column and table unique overlap',
    async (dbType, quote) => {
    const knex = {
      raw: vi.fn().mockResolvedValue({ rows: [] }),
      schema: { hasColumn: vi.fn().mockResolvedValue(false) },
      client: { config: { client: dbType === 'postgres' ? 'pg' : 'mysql2' } },
    } as any;

    const statements = await generateSQLFromDiff(
      knex,
      'ai_user_config',
      {
        columns: {
          create: [
            {
              name: 'userId',
              type: 'uuid',
              isNullable: false,
              isUnique: true,
              isForeignKey: true,
              foreignKeyTarget: 'enfyra_user',
            },
          ],
        },
        constraints: { uniques: { create: [['userId']] } },
      },
      dbType,
    );

    expect(
      statements.filter((statement) =>
        statement.includes(
          `CONSTRAINT ${quote}uq_ai_user_config_userId${quote} UNIQUE`,
        ),
      ),
    ).toHaveLength(1);
    },
  );

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
