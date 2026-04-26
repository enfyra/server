import { describe, it, expect, vi } from 'vitest';
import { applyAlterColumnType } from 'src/engine/knex';

function makeAlterTableStub() {
  const calls: Array<{ method: string; args: any[] }> = [];
  const columnBuilder = {
    alter: vi.fn().mockReturnValue({ notNullable: vi.fn(), nullable: vi.fn() }),
  };
  const table = {
    integer: vi.fn().mockImplementation((name: string) => {
      calls.push({ method: 'integer', args: [name] });
      return columnBuilder;
    }),
    string: vi.fn().mockImplementation((name: string, len?: number) => {
      calls.push({ method: 'string', args: [name, len] });
      return columnBuilder;
    }),
  } as any;
  return { table, calls, columnBuilder };
}

describe('applyAlterColumnType', () => {
  it("calls table.integer().alter() for 'integer'", () => {
    const { table, calls, columnBuilder } = makeAlterTableStub();
    const result = applyAlterColumnType(table, 'integer', 'age', 'users');
    expect(calls).toEqual([{ method: 'integer', args: ['age'] }]);
    expect(columnBuilder.alter).toHaveBeenCalledTimes(1);
    expect(result).toBeDefined();
  });

  it("calls table.string(name, 255).alter() for 'string'", () => {
    const { table, calls, columnBuilder } = makeAlterTableStub();
    applyAlterColumnType(table, 'string', 'name', 'users');
    expect(calls).toEqual([{ method: 'string', args: ['name', 255] }]);
    expect(columnBuilder.alter).toHaveBeenCalledTimes(1);
  });

  it("throws clear error for unsupported 'boolean'", () => {
    const { table } = makeAlterTableStub();
    expect(() =>
      applyAlterColumnType(table, 'boolean', 'isActive', 'users'),
    ).toThrow(
      /ALTER COLUMN to type "boolean" is not supported.*"isActive".*"users"/,
    );
  });

  it("throws for 'uuid'", () => {
    const { table } = makeAlterTableStub();
    expect(() => applyAlterColumnType(table, 'uuid', 'id', 'sessions')).toThrow(
      /ALTER COLUMN to type "uuid" is not supported/,
    );
  });

  it("throws for 'datetime'", () => {
    const { table } = makeAlterTableStub();
    expect(() =>
      applyAlterColumnType(table, 'datetime', 'createdAt', 'logs'),
    ).toThrow(/ALTER COLUMN to type "datetime" is not supported/);
  });

  it("throws for 'timestamp'", () => {
    const { table } = makeAlterTableStub();
    expect(() => applyAlterColumnType(table, 'timestamp', 't', 'logs')).toThrow(
      /ALTER COLUMN to type "timestamp" is not supported/,
    );
  });

  it("throws for 'bigInteger'", () => {
    const { table } = makeAlterTableStub();
    expect(() =>
      applyAlterColumnType(table, 'bigInteger', 'count', 'stats'),
    ).toThrow(/not supported/);
  });

  it("throws for 'enum'", () => {
    const { table } = makeAlterTableStub();
    expect(() =>
      applyAlterColumnType(table, 'enum', 'status', 'orders'),
    ).toThrow(/not supported/);
  });

  it('error message contains migration guidance', () => {
    const { table } = makeAlterTableStub();
    try {
      applyAlterColumnType(table, 'json', 'meta', 'logs');
      throw new Error('should have thrown');
    } catch (e: any) {
      expect(e.message).toContain('Drop and recreate');
      expect(e.message).toContain('sync-table.ts');
    }
  });

  it('does not call table builder when throwing', () => {
    const { table, calls } = makeAlterTableStub();
    expect(() => applyAlterColumnType(table, 'unknown', 'x', 't')).toThrow();
    expect(calls).toEqual([]);
    expect(table.integer).not.toHaveBeenCalled();
    expect(table.string).not.toHaveBeenCalled();
  });
});
