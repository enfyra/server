import { Buffer } from 'node:buffer';
import { describe, expect, it } from 'vitest';
import {
  getForeignKeyColumnName,
  getJunctionTableName,
  getShortFkConstraintName,
  getShortFkName,
  getShortIndexName,
  getShortPkName,
} from '@enfyra/kernel';

describe('SQL schema naming contracts', () => {
  it('enforces PostgreSQL identifier limits in UTF-8 bytes', () => {
    const name = `${'界'.repeat(21)}x`;
    const pkName = getShortPkName(name);
    expect(Buffer.byteLength(pkName, 'utf8')).toBeLessThanOrEqual(63);
  });

  it('bounds relation columns, constraints, and indexes without renaming short forms', () => {
    expect(getForeignKeyColumnName('author')).toBe('authorId');
    expect(getShortFkName('posts', 'author', 'src')).toBe(
      'posts_author_src_fk',
    );
    expect(getShortIndexName('posts', 'author', 'src')).toBe(
      'posts_author_src_idx',
    );

    const longA = `${'界'.repeat(30)}a`;
    const longB = `${'界'.repeat(30)}b`;
    for (const name of [
      getForeignKeyColumnName(longA),
      getShortFkName(longA, longB, 'src'),
      getShortIndexName(longA, longB, 'src'),
    ]) {
      expect(Buffer.byteLength(name, 'utf8')).toBeLessThanOrEqual(63);
    }
    expect(getForeignKeyColumnName(longA)).not.toBe(
      getForeignKeyColumnName(longB),
    );
    expect(getShortIndexName(longA, `${longB}a`, 'src')).not.toBe(
      getShortIndexName(longA, `${longB}b`, 'src'),
    );
  });

  it('keeps source and target constraint directions distinct', () => {
    const table = 'junction_table_with_a_name_long_enough_to_require_shortening';
    expect(getShortFkConstraintName(table, 'sourceId', 'src')).not.toBe(
      getShortFkConstraintName(table, 'targetId', 'tgt'),
    );
  });

  it('uses an unambiguous tuple encoding without renaming ordinary junctions', () => {
    expect(getJunctionTableName('posts', 'tags', 'tags')).toBe(
      'j_ee1e52bc2158',
    );
    expect(getJunctionTableName('a:b', 'c', 'd')).not.toBe(
      getJunctionTableName('a', 'b:c', 'd'),
    );
  });
});
