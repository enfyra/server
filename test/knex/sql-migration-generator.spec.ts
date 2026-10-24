import { describe, expect, it, vi } from 'vitest';
import type { Knex } from 'knex';
import { executeBatchSQL } from '../../src/engines/knex/utils/migration/sql-diff-generator';
import {
  generateColumnDefinition,
  generateModifyColumnSQL,
  supportsSqlColumnDefault,
} from '../../src/engines/knex';

describe('SQL migration column generator', () => {
  it('retains PostgreSQL default and check captures while separating the physical type', () => {
    expect(
      generateModifyColumnSQL(
        'item',
        'score',
        'INTEGER CHECK (score > 0) NOT NULL DEFAULT 7',
        'postgres',
      ),
    ).toEqual([
      'ALTER TABLE "item" ALTER COLUMN "score" TYPE INTEGER',
      'ALTER TABLE "item" ALTER COLUMN "score" SET NOT NULL',
      'ALTER TABLE "item" ALTER COLUMN "score" SET DEFAULT 7',
      'ALTER TABLE "item" ADD CONSTRAINT chk_item_score CHECK ((score > 0))',
    ]);
  });

  it('executes migration statements in order and compensates completed statements in reverse order', async () => {
    const statements = [
      'ALTER TABLE "item" ADD COLUMN "title" VARCHAR(255)',
      'CREATE INDEX "item_title_idx" ON "item" ("title")',
      'ALTER TABLE "item" ADD COLUMN "fail" INTEGER',
    ];
    const executed: string[] = [];
    const rolledBack: string[] = [];
    const failure = new Error('migration failed');
    const knex = {
      client: { config: { client: 'mysql2' } },
      transaction: vi.fn(async (callback: (connection: object) => Promise<void>) =>
        callback({
          raw: vi.fn(async (statement: string) => {
            if (statement.startsWith('SET SESSION')) return;
            executed.push(statement);
            if (statement.includes('"fail"')) throw failure;
          }),
        }),
      ),
      raw: vi.fn(async (statement: string) => {
        rolledBack.push(statement);
      }),
    } as unknown as Knex;
    const journal = { uuid: 'migration-test', markFailed: vi.fn(async () => undefined) };

    await expect(
      executeBatchSQL(knex, statements.join(';'), 'mysql', undefined, journal),
    ).rejects.toBe(failure);
    expect(executed).toEqual(statements);
    expect(rolledBack).toEqual([
      'DROP INDEX `ITEM_TITLE_IDX` ON `ITEM`',
      'ALTER TABLE `ITEM` DROP COLUMN `TITLE`',
    ]);
    expect(journal.markFailed).toHaveBeenCalledWith('migration failed');
  });

  it('omits MySQL defaults for text-like column types', () => {
    expect(
      generateColumnDefinition(
        {
          name: 'body',
          type: 'text',
          isNullable: false,
          defaultValue: '',
        },
        'mysql',
      ),
    ).toBe('TEXT NOT NULL');

    expect(
      generateColumnDefinition(
        {
          name: 'config',
          type: 'simple-json',
          isNullable: false,
          defaultValue: {},
        },
        'mysql',
      ),
    ).toBe('LONGTEXT NOT NULL');
  });

  it('keeps defaults for MySQL scalar column types', () => {
    expect(
      generateColumnDefinition(
        {
          name: 'title',
          type: 'varchar',
          defaultValue: "owner's copy",
        },
        'mysql',
      ),
    ).toBe("VARCHAR(255) DEFAULT 'owner''s copy'");

    expect(
      generateColumnDefinition(
        {
          name: 'active',
          type: 'boolean',
          isNullable: false,
          defaultValue: false,
        },
        'mysql',
      ),
    ).toBe('BOOLEAN NOT NULL DEFAULT 0');
  });

  it('maps longtext to the largest portable text storage', () => {
    expect(
      generateColumnDefinition(
        {
          name: 'sourceCode',
          type: 'longtext',
          isNullable: false,
        },
        'mysql',
      ),
    ).toBe('LONGTEXT NOT NULL');
    expect(
      generateColumnDefinition(
        {
          name: 'sourceCode',
          type: 'longtext',
          isNullable: false,
        },
        'postgres',
      ),
    ).toBe('TEXT NOT NULL');
  });

  it('allows text defaults on PostgreSQL', () => {
    expect(supportsSqlColumnDefault({ type: 'text' }, 'postgres')).toBe(true);
    expect(
      generateColumnDefinition(
        {
          name: 'body',
          type: 'text',
          isNullable: false,
          defaultValue: '',
        },
        'postgres',
      ),
    ).toBe("TEXT NOT NULL DEFAULT ''");
  });

  it('maps float columns to numeric SQL types', () => {
    expect(
      generateColumnDefinition(
        {
          name: 'amount_usd',
          type: 'float',
          isNullable: false,
        },
        'postgres',
      ),
    ).toBe('DOUBLE PRECISION NOT NULL');

    expect(
      generateColumnDefinition(
        {
          name: 'amount_usd',
          type: 'float',
        },
        'mysql',
      ),
    ).toBe('FLOAT');
  });
});
