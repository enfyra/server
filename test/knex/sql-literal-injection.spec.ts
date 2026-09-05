import { describe, expect, it, vi } from 'vitest';
import { generateColumnDefinition } from '../../src/engines/knex/utils/migration/sql-generator';
import { planPostgresEnumTypeCreation } from '../../src/engines/knex/utils/migration/postgres-enum-migration';

describe('Enum literal injection protection', () => {
  it('escapes single quotes in enum options for mysql', () => {
    const col = { name: 'status', type: 'enum', options: ['active', "it's"] };
    const result = generateColumnDefinition(col as any, 'mysql');
    expect(result).toContain("'it''s'");
    expect(result).not.toContain("'it's'");
  });

  it('escapes injection attempts in native PostgreSQL enum options', async () => {
    const col = {
      name: 'status',
      type: 'enum',
      options: ["x'); DROP TABLE enfyra_user; --"],
    };
    const plan = await planPostgresEnumTypeCreation(
      { raw: vi.fn().mockResolvedValue({ rows: [] }) } as any,
      'orders',
      col,
    );
    expect(plan.statements[0]).toContain("x''); DROP TABLE enfyra_user; --'");
    expect(plan.statements[0]).toContain('CREATE TYPE');
  });

  it('rejects PostgreSQL enum generation without native type planning', () => {
    expect(() =>
      generateColumnDefinition(
        { name: 'status', type: 'enum', options: ['active'] },
        'postgres',
      ),
    ).toThrow(/requires native enum planning/);
  });

  it('escapes injection attempt in enum options for mysql', () => {
    const col = {
      name: 'status',
      type: 'enum',
      options: ["x'); DROP TABLE enfyra_user; --"],
    };
    const result = generateColumnDefinition(col as any, 'mysql');
    expect(result).toContain("x''); DROP TABLE enfyra_user; --'");
    expect(result).toContain('ENUM');
  });

  it('handles normal enum options without modification', () => {
    const col = {
      name: 'status',
      type: 'enum',
      options: ['active', 'inactive', 'pending'],
    };
    const result = generateColumnDefinition(col as any, 'mysql');
    expect(result).toContain("'active'");
    expect(result).toContain("'inactive'");
    expect(result).toContain("'pending'");
  });
});

describe('DEFAULT value injection protection', () => {
  it('escapes string default values', () => {
    const col = { name: 'note', type: 'varchar', defaultValue: "it's a test" };
    const result = generateColumnDefinition(col as any, 'postgres');
    expect(result).toContain("DEFAULT 'it''s a test'");
  });

  it('handles numeric default without quotes', () => {
    const col = { name: 'count', type: 'int', defaultValue: 42 };
    const result = generateColumnDefinition(col as any, 'postgres');
    expect(result).toContain('DEFAULT 42');
  });

  it('handles boolean default for postgres', () => {
    const col = { name: 'active', type: 'boolean', defaultValue: true };
    const result = generateColumnDefinition(col as any, 'postgres');
    expect(result).toContain('DEFAULT true');
  });

  it('handles boolean default for mysql', () => {
    const col = { name: 'active', type: 'boolean', defaultValue: false };
    const result = generateColumnDefinition(col as any, 'mysql');
    expect(result).toContain('DEFAULT 0');
  });

  it('escapes non-string non-number non-boolean default defensively', () => {
    const col = {
      name: 'x',
      type: 'varchar',
      defaultValue: { toString: () => "evil'; DROP TABLE x; --" },
    };
    const result = generateColumnDefinition(col as any, 'postgres');
    expect(result).toContain("''");
    expect(result).not.toMatch(/DEFAULT [^']evil/);
  });
});
