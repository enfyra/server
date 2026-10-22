import { describe, expect, it } from 'vitest';
import { Double, Int32, Long, ObjectId } from 'mongodb';
import {
  normalizeCloneSqlTimestamp,
  normalizeCloneValue,
} from '../e2e/utils/production-clone-value.util';

describe('production clone preservation values', () => {
  it('normalizes BSON scalars without recursive Long.valueOf calls or precision loss', () => {
    expect(
      normalizeCloneValue({
        integer: new Int32(3),
        float: new Double(1.25),
        large: Long.fromString('9007199254740993'),
        id: new ObjectId('123456789012345678901234'),
      }),
    ).toEqual({
      integer: 3,
      float: 1.25,
      large: '9007199254740993',
      id: '123456789012345678901234',
    });
  });
  it('compares equivalent UTC forms with full microsecond precision', () => {
    expect(normalizeCloneSqlTimestamp('2026-10-22 12:34:56.123456+00')).toBe(
      normalizeCloneSqlTimestamp('2026-10-22 12:34:56.123456'),
    );
    expect(normalizeCloneSqlTimestamp('2026-10-22 12:34:56.123456')).not.toBe(
      normalizeCloneSqlTimestamp('2026-10-22 12:34:56.123457'),
    );
    expect(normalizeCloneSqlTimestamp('2026-10-22 12:34:56+00')).toBe(
      '2026-10-22T12:34:56.000000Z',
    );
  });
  it('rejects non-UTC temporal input instead of silently shifting an instant', () => {
    expect(() => normalizeCloneSqlTimestamp('2026-10-22 12:34:56+07')).toThrow(
      'UTC',
    );
  });
});
