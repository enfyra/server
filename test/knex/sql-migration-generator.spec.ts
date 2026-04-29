import { describe, expect, it } from 'vitest';
import {
  generateColumnDefinition,
  supportsSqlColumnDefault,
} from '../../src/engines/knex';

describe('SQL migration column generator', () => {
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
});
