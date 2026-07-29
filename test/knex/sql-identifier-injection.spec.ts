import { describe, expect, it } from 'vitest';
import { quoteIdentifier } from '../../src/engines/knex/utils/migration/sql-dialect';

describe('quoteIdentifier escape protection', () => {
  it('escapes embedded double quotes for postgres', () => {
    const result = quoteIdentifier('col"name', 'postgres');
    expect(result).toBe('"col""name"');
  });

  it('escapes embedded backticks for mysql', () => {
    const result = quoteIdentifier('col`name', 'mysql');
    expect(result).toBe('`col``name`');
  });

  it('escapes injection attempt in postgres', () => {
    const result = quoteIdentifier('id"; DROP TABLE enfyra_user; --', 'postgres');
    expect(result).toBe('"id""; DROP TABLE enfyra_user; --"');
  });

  it('escapes injection attempt in mysql', () => {
    const result = quoteIdentifier('id`; DROP TABLE enfyra_user; --', 'mysql');
    expect(result).toBe('`id``; DROP TABLE enfyra_user; --`');
  });

  it('handles normal identifier without modification', () => {
    expect(quoteIdentifier('user_name', 'postgres')).toBe('"user_name"');
    expect(quoteIdentifier('user_name', 'mysql')).toBe('`user_name`');
  });

  it('handles multiple embedded delimiters', () => {
    expect(quoteIdentifier('a"b"c', 'postgres')).toBe('"a""b""c"');
    expect(quoteIdentifier('a`b`c', 'mysql')).toBe('`a``b``c`');
  });
});

describe('Column name validation regex', () => {
  const COLUMN_NAME_REGEX = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

  it('accepts snake_case', () => {
    expect(COLUMN_NAME_REGEX.test('my_column')).toBe(true);
  });

  it('accepts camelCase', () => {
    expect(COLUMN_NAME_REGEX.test('myColumn')).toBe(true);
  });

  it('accepts PascalCase', () => {
    expect(COLUMN_NAME_REGEX.test('MyColumn')).toBe(true);
  });

  it('accepts underscore prefix', () => {
    expect(COLUMN_NAME_REGEX.test('_private')).toBe(true);
  });

  it('accepts single letter', () => {
    expect(COLUMN_NAME_REGEX.test('x')).toBe(true);
  });

  it('rejects embedded double quote', () => {
    expect(COLUMN_NAME_REGEX.test('col"name')).toBe(false);
  });

  it('rejects embedded backtick', () => {
    expect(COLUMN_NAME_REGEX.test('col`name')).toBe(false);
  });

  it('rejects semicolon injection', () => {
    expect(COLUMN_NAME_REGEX.test('id"; DROP TABLE x; --')).toBe(false);
  });

  it('rejects space', () => {
    expect(COLUMN_NAME_REGEX.test('my column')).toBe(false);
  });

  it('rejects dash', () => {
    expect(COLUMN_NAME_REGEX.test('my-column')).toBe(false);
  });

  it('rejects leading digit', () => {
    expect(COLUMN_NAME_REGEX.test('1column')).toBe(false);
  });

  it('rejects empty string', () => {
    expect(COLUMN_NAME_REGEX.test('')).toBe(false);
  });

  it('rejects null byte', () => {
    expect(COLUMN_NAME_REGEX.test('col\x00name')).toBe(false);
  });
});
