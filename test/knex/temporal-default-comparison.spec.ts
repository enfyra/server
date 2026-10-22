import { describe, expect, it } from 'vitest';
import { compareSchemas } from '../../src/engines/knex/utils/provision/schema-comparison';

describe('temporal function default comparison', () => {
  it.each([
    'CURRENT_TIMESTAMP',
    'CURRENT_TIMESTAMP()',
    ...Array.from(
      { length: 7 },
      (_, precision) => `CURRENT_TIMESTAMP(${precision})`,
    ),
    'now()',
    'NOW(6)',
  ])('accepts the current instant default %s', (currentDefault) => {
    const diff = compareSchemas(
      {
        tableName: 'sessions',
        junctionTables: [],
        definition: {
          name: 'sessions',
          columns: [
            {
              name: 'expiresAt',
              type: 'datetime',
              isNullable: true,
              defaultValue: 'now',
            },
          ],
          relations: [],
        },
      },
      {
        columns: [
          {
            name: 'expiresAt',
            type: 'datetime',
            isNullable: true,
            defaultValue: currentDefault,
          },
        ],
        foreignKeys: [],
        uniques: [],
        indexes: [],
      },
      'mysql2',
    );
    expect(diff.columnsToModify).toEqual([]);
  });
  it('does not accept an arbitrary function expression', () => {
    const diff = compareSchemas(
      {
        tableName: 'sessions',
        junctionTables: [],
        definition: {
          name: 'sessions',
          columns: [
            {
              name: 'expiresAt',
              type: 'datetime',
              isNullable: true,
              defaultValue: 'now',
            },
          ],
          relations: [],
        },
      },
      {
        columns: [
          {
            name: 'expiresAt',
            type: 'datetime',
            isNullable: true,
            defaultValue: 'CURRENT_TIMESTAMP(6) + INTERVAL 1 DAY',
          },
        ],
        foreignKeys: [],
        uniques: [],
        indexes: [],
      },
      'mysql2',
    );
    expect(diff.columnsToModify[0].changes).toContain('default');
  });
});
