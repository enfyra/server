import { describe, expect, it } from 'vitest';
import { Long } from 'mongodb';
import { coerceMongoLongWriteValue } from '../../src/engines/mongo/utils/mongo-long-write.util';
import {
  normalizeBsonLongs,
  normalizeMongoDocument,
} from '../../src/engines/mongo/utils/normalize-mongo-document.util';

describe('Mongo 64-bit integer boundary', () => {
  it('coerces a write value into a BSON Long on both accepted forms', () => {
    expect(Long.isLong(coerceMongoLongWriteValue(42))).toBe(true);
    expect(String(coerceMongoLongWriteValue('9223372036854775807'))).toBe(
      '9223372036854775807',
    );
  });

  it('leaves a value the validator must reject untouched', () => {
    expect(coerceMongoLongWriteValue('not-an-integer')).toBe('not-an-integer');
    expect(coerceMongoLongWriteValue(1.5)).toBe(1.5);
  });

  it('reads a BSON Long back as a decimal string that survives JSON', () => {
    const value = Long.fromString('9223372036854775807');
    expect(normalizeBsonLongs({ filesize: value })).toEqual({
      filesize: '9223372036854775807',
    });
  });

  it('normalizes a Long nested inside a document and an array', () => {
    const value = Long.fromString('9007199254740993');
    expect(
      normalizeMongoDocument({ nested: { id: value }, list: [value] }),
    ).toEqual({
      nested: { id: '9007199254740993' },
      list: ['9007199254740993'],
    });
  });

  it('keeps ordinary primitives and ObjectIds intact', () => {
    expect(normalizeBsonLongs({ count: 3, name: 'x', flag: false })).toEqual({
      count: 3,
      name: 'x',
      flag: false,
    });
  });
});
