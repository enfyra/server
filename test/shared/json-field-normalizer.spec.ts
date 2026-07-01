import { describe, expect, it } from 'vitest';
import {
  normalizeJsonFieldValue,
  stringifyJsonFieldValue,
} from '../../src/shared/utils/json-field-normalizer.util';

describe('json field normalizer', () => {
  it('unwraps repeatedly stringified JSON metadata values', () => {
    let value: any = null;
    for (let i = 0; i < 12; i += 1) {
      value = JSON.stringify(value);
    }

    expect(normalizeJsonFieldValue(value)).toBeNull();
  });

  it('preserves structured objects and stores one JSON layer', () => {
    const value = { richText: { toolbar: ['bold'] } };

    expect(normalizeJsonFieldValue(value)).toEqual(value);
    expect(stringifyJsonFieldValue(JSON.stringify(value))).toBe(
      JSON.stringify(value),
    );
  });

  it('leaves non-JSON strings intact', () => {
    expect(normalizeJsonFieldValue('plain text')).toBe('plain text');
    expect(stringifyJsonFieldValue('plain text')).toBe('"plain text"');
  });
});
